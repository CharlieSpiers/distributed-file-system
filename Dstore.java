import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

class Dstore {

    private ControllerConnection controller;
    private int timeout;
    private File folder;
    private ServerSocket serverSocket;
    private ArrayList<ClientConnection> clientsList;
    private HashMap<String, File> fileIndex;
    private HashMap<String, Integer> fileSizes;
    private HashMap<String, ReentrantLock> fileLocks;
    private final ReentrantLock rebalanceLock = new ReentrantLock();

    public static void main(String[] args) {
        if (args.length != 4) {
           System.out.println("Arguments should be formatted like so:\nDstore port cport timeout file_folder");
        }
        new Dstore(args[0], args[1], args[2], args[3]);
    }
    
    public Dstore(String portStr, String cportStr, String timeoutStr, String file_folder) {
        if (portStr.isEmpty() || cportStr.isEmpty() || timeoutStr.isEmpty() || file_folder.isEmpty()) {
            Logger.err("Argument was empty.", new Exception(), this);
            return;
        }

        int port, cport;
        clientsList = new ArrayList<>();
        fileIndex = new HashMap<>();
        fileLocks = new HashMap<>();
        fileSizes = new HashMap<>();

        try {
            port = Integer.parseInt(portStr);
            cport = Integer.parseInt(cportStr);
            timeout = Integer.parseInt(timeoutStr);
        } catch (NumberFormatException e) {
            Logger.err("Could not create the Dstore, incorrect argument format", e, this);
            return;
        } 
        
        try { setupFolder(folder = new File(file_folder)); } 
        catch (Exception e) { 
            Logger.err("Problem creating the file folder: " + file_folder, e, this);
            return;
        }

        /**
         * Processes should send textual messages (e.g., LIST – see below) using the println()
            method of PrintWriter class, and receive using the readLine() method of BufferedReader
            class. For data messages (i.e., file content), processes should send using the write()
            method of OutputStream class and receive using the readNBytes() method of
            InputStream class.
         */
        Logger.info("Setup complete, starting connections", this);
        try {
            Socket socket = new Socket("localhost", cport);
            controller = new ControllerConnection(socket, port, this);
            awaitClientConnections(port);
        } catch (Exception e) {
            Logger.err("A problem occured while trying to set up connections", e, this);
        }
    }

    /**
     * Attempt to store the file
     * @param file The byte array that represents the file
     * @param fileName Name of file
     * @param fileSize Size of file (in bytes)
     */
    public void storeFile(byte[] fileBytes, String fileName, int fileSize, boolean ack) {
        try {
            rebalanceLock.lock();
            rebalanceLock.unlock();
            ReentrantLock fileLock;
            File newFile = new File(folder, fileName);
            synchronized (fileIndex) { fileIndex.put(fileName, newFile); }
            synchronized (fileLocks) { fileLocks.put(fileName, fileLock = new ReentrantLock()); }
            synchronized (fileSizes) { fileSizes.put(fileName, fileSize); }
            fileLock.lock();
            FileOutputStream fs = new FileOutputStream(newFile);
            fs.write(fileBytes);
            fs.close();
            if (ack) controller.sendMessage("STORE_ACK " + fileName);
            fileLock.unlock();
        } catch (Exception e) {
            Logger.err("Storing file failed.", e, this);
        }
    }

    /**
     * Load a file, return on error or completion
     * @param fileName Name of file to load
     * @param client Client to send data to
     */
    public void loadFile(String fileName, OutputStream outStream) throws Exception {
        byte[] data;
        synchronized (fileIndex) {
            FileInputStream fStream = new FileInputStream(fileIndex.get(fileName));
            data = fStream.readAllBytes();
            fStream.close();
        }
        outStream.write(data);
    }

    /**
     * Remove a file from the Dstore
     * @param fileName File to remove
     */
    public void removeFile(String fileName, boolean ack) {
        rebalanceLock.lock();
        rebalanceLock.unlock();
        fileLocks.get(fileName).lock();
        synchronized (fileIndex) {    
            if (!fileIndex.keySet().contains(fileName)) {
                if (ack) controller.sendMessage("ERROR_FILE_DOES_NOT_EXIST " + fileName);
                fileLocks.get(fileName).unlock();
                return;
            }
            if (fileIndex.get(fileName).delete()) {
                fileIndex.remove(fileName);
                if (ack) controller.sendMessage("REMOVE_ACK " + fileName);
            } else {
                Logger.err("Could not delete the file: " + fileName, new Exception(), this);
                fileLocks.get(fileName).unlock();
                return;
            }
            fileLocks.get(fileName).unlock();
            fileLocks.remove(fileName);
        }
        
    }

    /**
     * Reply to the controller with all of the files that this Dstore has stored.
     */
    public void listFiles() {
        String files = fileIndex.keySet().stream().collect(Collectors.joining(" "));
        controller.sendMessage("LIST " + files);
    }

    public Boolean sendFile(String fileName, Integer port) {
        fileLocks.get(fileName).lock();
        try (Socket store = new Socket("localhost", port);
            BufferedReader br = new BufferedReader(new InputStreamReader(store.getInputStream()));
            PrintWriter pr = new PrintWriter(new OutputStreamWriter(store.getOutputStream()), true);) {
            store.setSoTimeout(timeout);
            pr.println("REBALANCE_STORE " + fileName + " " + fileSizes.get(fileName));
            if (br.readLine().equals("ACK")) loadFile(fileName, store.getOutputStream());
        } catch (Exception e) {
            Logger.err("Could not send file to the other Dstore " + port, e, this);
            return false;
        } finally { fileLocks.get(fileName).unlock(); }    
        return true;
    }

    /**
     * Create / empty the file (which should be a directory).
     * @param folder A File which references the folder that you want to use. It is emptied / created.
     * @throws FileSystemException If the folder is unable to be emptied or created.
     * @throws SecurityException If a security manager exists and it denies access to a file.
     */
    private void setupFolder(File folder) throws FileNotFoundException, SecurityException {
        if (folder.exists() && folder.isDirectory()) {
            emptyDirectory(folder);
            return;
        }

        if (!folder.mkdir()) throw new FileNotFoundException("Could not create new folder at this location: " + folder.getPath());
    }

    /**
     * Empty a folder (recursively)
     * @param directory The File() instance which points to the folder you want to empty
     * @throws SecurityException If a security manager exists and it denies access to a file.
     */
    private void emptyDirectory(File directory) throws SecurityException {
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) emptyDirectory(file);
            file.delete();
        }
    }

    /**
     * Start a new Thread which just listens for new connections from clients and accepts them.
     * @param port The port that the server socket will be stationed on.
     */
    private void awaitClientConnections(int port) {
        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(port);
                int clients = 0;
                while (true) {
                    Socket client = serverSocket.accept();
                    client.setSoTimeout(timeout);
                    try { clientsList.add(new ClientConnection(client, "Client"+clients, this)); } 
                    catch (IOException e) { Logger.err("Something went wrong setting up the client connection", e, this); }
                    clients++;
                }
            } catch (SocketException e) { Logger.info("Server socket closed", this); }
            catch (Exception e) { 
                Logger.err("The Dstores serverSocket closed, meaning nothing new can connect to it. Shutting down", e, this);
                this.closeAll();
            }
        }, "ClientAcceptor").start();
    }
    
    /**
     * Called once the Controller stops responding. Closes all connections and threads.
     */
    public void closeAll() {
        Logger.info("Closing clients", this);
        ArrayList<ClientConnection> cli = new ArrayList<>(clientsList);
        for (ClientConnection client : cli) removeClient(client);

        try { serverSocket.close(); }
        catch (Exception e) { Logger.err("Server socket could not be closed", e, this); }
    }

    /**
     * Remove a client from the list of clients. Called once a request has finished being handled.
     * @param client Client to close connection to.
     */
    public void removeClient(ClientConnection client) {
        clientsList.remove(client);
        client.close();
    }

    /**
     * Handle the connection to the controller
     */
    private class ControllerConnection extends ConnectionThread<Dstore> {
        
        public ControllerConnection(Socket socket, int port, Dstore dstore) throws IOException {
            super(socket, "Dstore"+port, dstore);
            sendMessage("JOIN " + port);
        }

        @Override
        public void close() {
            super.close();
            server.closeAll();
        }

        public void reveiveMessage(String message) {
            if (message.startsWith("REMOVE")) requestRemove(message);
            if (message.equals("LIST")) server.listFiles();
            if (message.startsWith("REBALANCE")) rebalance(message);
        }

        private void rebalance(String message) {
            rebalanceLock.lock();
            try {
                ArrayList<String> segments = new ArrayList<>(Arrays.asList(message.split(" ", 2)[1].split(" ")));
                Integer filesToSendNum = Integer.parseInt(segments.get(0));
                segments.remove(0);

                HashMap<String, ArrayList<Integer>> filesToSend = new HashMap<>();
                for (int i = 0; i < filesToSendNum; i++) {
                    String fileName = segments.get(0);
                    segments.remove(0);
                    ArrayList<Integer> dStores = new ArrayList<>();
                    filesToSend.put(fileName, dStores);
                    Integer numOfDstores = Integer.parseInt(segments.get(0));
                    segments.remove(0);
                    for (int j = 0; j < numOfDstores; j++) {
                        dStores.add(Integer.parseInt(segments.get(0)));
                        segments.remove(0);
                    }
                }

                ArrayList<Boolean> sendsComplete = new ArrayList<>();
                for (String fileName : filesToSend.keySet()) {
                    for (Integer port : filesToSend.get(fileName)) {
                        new Thread(() -> { sendsComplete.add(server.sendFile(fileName, port)); }).start();
                    }
                }

                for (ReentrantLock lock : fileLocks.values()) {
                    lock.lock();
                    lock.unlock();
                }

                if (sendsComplete.contains(false)) {
                    Logger.info("A send wasn't able to complete.", this);
                    return;
                }

                //Leftover segments after removing the number of files to remove are the files.
                segments.remove(0);
                for (String fileName : segments) {
                    removeFile(fileName, false);
                }
                server.controller.sendMessage("REBALANCE_COMPLETE");
            } catch (Exception e) {
                Logger.err("Message was malformed", e, this);
            } finally { rebalanceLock.unlock(); }
        }

        private void requestRemove(String message) {
            try {
                String fileName = message.split(" ")[1];
                server.removeFile(fileName, true);
            } catch (Exception e) {
                Logger.err("Message was not in the correct format", e, this);
            }
        }
    }

    /**
     * Handle incoming and outgoing messages from a client
     */
    private class ClientConnection extends ConnectionThread<Dstore> {
        
        public ClientConnection(Socket socket, String name, Dstore dstore) throws IOException {
            super(socket, name, dstore);
            Logger.info("Connection made", this);
            server.clientsList.add(this);
        }

        public void reveiveMessage(String message) {
            try {
                if (message.startsWith("STORE")) storeRequest(message, true);
                if (message.startsWith("REBALANCE_STORE")) storeRequest(message, false);
                if (message.startsWith("LOAD_DATA")) loadRequest(message);
            } catch (IOException e) { Logger.err("Load could not be performed", e, this); } 
            catch (Exception e) {  Logger.err("Message was not in the correct format", e, this); } 
            finally { server.removeClient(this); }
        }

        private void loadRequest(String message) throws Exception {
            String fileName = message.split(" ")[1];
            server.loadFile(fileName, socket.getOutputStream());
        }

        private void storeRequest(String message, boolean ack) throws Exception {
            String fileName = message.split(" ")[1];
            Integer fileSize = Integer.parseInt(message.split(" ")[2]);

            sendMessage("ACK");
            var file = new byte[fileSize];
            socket.getInputStream().readNBytes(file, 0, fileSize);
            server.storeFile(file, fileName, fileSize, ack);
        }
    }
}