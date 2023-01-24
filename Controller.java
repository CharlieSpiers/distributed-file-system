import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

class Controller {
    private final HashMap<String, CountDownLatch> expectedStoreAcks = new HashMap<>();
    private final HashMap<String, CountDownLatch> expectedRemoveAcks = new HashMap<>();
    
    private CountDownLatch expectedRebalanceAcks = new CountDownLatch(0);
    private CountDownLatch expectedLists = new CountDownLatch(0);
    private HashMap<DstoreCon, ArrayList<String>> dStoreLists = new HashMap<>();
    
    private final FileIndex index = new FileIndex();
    private final ArrayList<ClientConnection> clients = new ArrayList<>();
    private final ReentrantLock rebalLock = new ReentrantLock();
    private final ScheduledThreadPoolExecutor rebalScheduler = new ScheduledThreadPoolExecutor(1);
    
    private int replicationFactor, timeout, cport, rebalance_period;

    public static void main(String[] args) {
        if (args.length != 4) System.out.println("Arguments should be formatted like so:\nController cport R timeout rebalance_period");
        new Controller(args[0], args[1], args[2], args[3]);
    }

    /**
     * Set up the controller and start listening for connections.
     * Handles any errors which occur due to the arguments being bad in this function.
     * @param cportStr port of the controller
     * @param replicationFactorStr replication factor
     * @param timeoutStr timeout (for Dstores to ack messages)
     * @param rebalance_periodStr Time between rebalances
     */
    public Controller(String cportStr, String replicationFactorStr, String timeoutStr, String rebalance_periodStr) {
        Logger.clearLogs();
        Logger.info("Creating new Controller", this);
        try { //Parse everything into integers
            cport = Integer.parseInt(cportStr);
            replicationFactor = Integer.parseInt(replicationFactorStr);
            timeout = Integer.parseInt(timeoutStr);
            rebalance_period = Integer.parseInt(rebalance_periodStr);
            if (cport < 1025 || cport > 65535 || replicationFactor < 1 || timeout < 0 || rebalance_period < 0) throw new Exception("An argument was out of the valid range");
        } catch (Exception e) {
            Logger.err("Command line argument was malformed", e, this);
            return;
        }

        //Create the ServerSocket and listen for connections, and start rebalancing.
        
        try (ServerSocket serverSocket = new ServerSocket(cport);) {
            rebalScheduler.schedule(() -> rebalance(), rebalance_period, TimeUnit.SECONDS);
            while(true) handleNewConnection(serverSocket.accept());
        } catch (Exception e) { Logger.err("Exception was thrown. The server socket was closed", e, this); }

        Logger.info("Shutting down executor service", this);
    }

    /**
     * In charge of getting an releasing locks either side of sendAndReceiveRebals
     */
    private void rebalance() {
        new Thread(() -> {
            rebalLock.lock();
            rebalScheduler.getQueue().clear();
            Logger.info("Rebalancing", this);

            try {checkState(null, null, false);} //Check for enough Dstores
            catch (Exception e) { 
                rebalScheduler.schedule(() -> rebalance(), rebalance_period, TimeUnit.SECONDS); 
                rebalLock.unlock(); 
                return; 
            }

            CountDownLatch clientLatch = new CountDownLatch(clients.size());
            for (ClientConnection client : clients) {
                new Thread(() -> {
                    try {
                        if (client.lock.isLocked()) Logger.info("Waiting for a process to finish", this);
                        client.lock();
                        clientLatch.countDown();
                        synchronized (clients) { clients.wait(); }
                        client.unlock();
                    } catch (Exception e) { Logger.err("Error trying to keep client locked", e, this); }
                }, "Client lock holder").start();
            }
            try {
                if (clientLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                    synchronized (index) { sendAndReceiveRebalances(); }
                } 
                else throw new Exception("Could not get all client locks in time");
            } catch (Exception e) { Logger.err("Rebalance could not be completed", e, this); }
            
            //Unlock the clients again when rebalancing is done
            rebalScheduler.schedule(() -> rebalance(), rebalance_period, TimeUnit.SECONDS);
            rebalLock.unlock();
            synchronized (clients) { clients.notifyAll(); }
            
        }, "Rebalance").start();
    }

    /**
     * Send and receive lists and rebalances
     * @throws Exception If anything fails
     */
    private void sendAndReceiveRebalances() throws Exception {
        //Client locks aquired, request lists from Dstores.
        ArrayList<DstoreCon> currStores = new ArrayList<>(index.getDStoreSet());
        dStoreLists = new HashMap<>();
        Set<String> fileSet = index.getFileSet().stream().filter(x -> index.getFileStatus(x).equals("store complete")).collect(Collectors.toSet());

        for (DstoreCon dStore : currStores) dStore.sendMessage("LIST");
        (expectedLists = new CountDownLatch(currStores.size())).await(timeout, TimeUnit.MILLISECONDS);

        index.getFileSet().stream().forEach(e -> { if (!dStoreLists.values().stream().anyMatch(x -> x.contains(e))) index.removeFile(e); });
        
        //Remove any files that aren't in the index
        ConcurrentHashMap<DstoreCon, ArrayList<String>> fileRemoves = new ConcurrentHashMap<>();
        ConcurrentHashMap<DstoreCon, ArrayList<String>> fileAdditions = new ConcurrentHashMap<>();
        for (DstoreCon dStore : dStoreLists.keySet()) {
            fileRemoves.put(dStore, new ArrayList<String>());
            fileAdditions.put(dStore, new ArrayList<String>());
            for (String listedFile : dStoreLists.get(dStore)) {
                if (!fileSet.contains(listedFile)) fileRemoves.get(dStore).add(listedFile);
            }
        }

        //Make sure each file should be in the Dstores R times
        for (String file : fileSet) {
            ArrayList<DstoreCon> currentDstores = new ArrayList<>(dStoreLists.keySet().stream().filter(x -> dStoreLists.get(x).contains(file)).toList());
            currentDstores.sort(Comparator.comparing(x -> dStoreLists.get(x).size()).reversed());
            Integer dStoresNeeded = replicationFactor-currentDstores.size();
            if (dStoresNeeded == 0) continue;
            //Gets Dstore list sorted to attempt to fill up the most empty Dstores first
            for (DstoreCon newStore : dStoreLists.keySet()) { //Has been sorted
                if (dStoresNeeded <= 0) break;
                if (currentDstores.contains(newStore)) continue;
                fileAdditions.get(newStore).add(file);
                dStoresNeeded--;
            }
            for (DstoreCon newStore : currentDstores) { //Ordered by largest amount of files first
                if (dStoresNeeded >= 0) break;
                fileRemoves.get(newStore).add(file);
                dStoresNeeded++;
            }
            if (dStoresNeeded != 0) throw new Exception("Could not ensure all files were replicated R times - less than R dstores LISTed");
        }

        float filesPerDstore = (float)(replicationFactor * fileSet.size()) / (float)dStoreLists.size();
        HashMap<DstoreCon, Float> fileRebalanceNum = new HashMap<>();
        float totalRebalDiff = 0;

        //Update dStoreLists (before reshuffle)
        for (DstoreCon dStore : fileRemoves.keySet()) dStoreLists.get(dStore).removeAll(fileRemoves.get(dStore));
        for (DstoreCon dStore : fileAdditions.keySet()) dStoreLists.get(dStore).addAll(fileAdditions.get(dStore));
        
        //Check that the total amount of files per Dstore adds up (aka totalRebalFactor == 0) and how many each Dstore needs.
        for (DstoreCon dStore : dStoreLists.keySet()) {
            float rebalanceFactor = filesPerDstore - dStoreLists.get(dStore).size();
            fileRebalanceNum.put(dStore, rebalanceFactor);
            totalRebalDiff += rebalanceFactor;
        }
        if (Math.abs(totalRebalDiff) > 1.0) throw new Exception("Total rebalance factor doesn't add up: " + totalRebalDiff);

        //Now that all Files that shouldn't exist have been removed, and we know how many files each Dstore needs
        boolean changed = true;
        while (true) {
            if (!changed) throw new Exception("Nothing changed in the last loop. Incorrect configuration of dStoreLists");
            changed = false;
            
            if (!fileRebalanceNum.values().stream().map(x -> (Math.abs(x) < 1)).toList().contains(false)) break;
            Comparator<DstoreCon> sort = Comparator.comparing(fileRebalanceNum::get);
            ArrayList<DstoreCon> dStores = new ArrayList<>(fileRebalanceNum.keySet().stream().sorted(sort).toList());
            DstoreCon needsRemove = dStores.get(0);
            DstoreCon needsFile = dStores.get(dStores.size()-1);
            String removedFile = null;

            //Find a file from the "bigger" Dstore to send to the "smaller"
            for (String file : dStoreLists.get(needsRemove)) {
                if (dStoreLists.get(needsFile).contains(file)) continue;
                fileAdditions.get(needsFile).add(file);
                dStoreLists.get(needsFile).add(file);
                float needsNum = fileRebalanceNum.get(needsFile);
                fileRebalanceNum.put(needsFile, needsNum-1);

                fileRemoves.get(needsRemove).add(file);
                removedFile = file;
                float removesNum = fileRebalanceNum.get(needsRemove);
                fileRebalanceNum.put(needsRemove, removesNum+1);
                changed = true;
                break;
            }
            if (removedFile == null) throw new Exception("Could not find a suitable file to send from a \"larger\" Dstore to a \"smaller\" one");
            dStoreLists.get(needsRemove).remove(removedFile);
        }

        //Craft messages for each of the Dstores
        int expectedAckNum = dStoreLists.keySet().size();
        for (DstoreCon dStore : dStoreLists.keySet()) {
            HashMap<String, ArrayList<Integer>> filesToSendMap = new HashMap<>();
            for (DstoreCon storeThatNeeds : fileAdditions.keySet()) {
                Function<String, Boolean> inOriginalList = x -> dStoreLists.get(dStore).contains(x) && !fileAdditions.get(dStore).contains(x);
                for (String file : fileAdditions.get(storeThatNeeds)) {
                    if (inOriginalList.apply(file)) { //If there is a Dstore that needs a file, and this one had that file at time of List, add it to the map.
                        ArrayList<Integer> a = new ArrayList<>();
                        a.add(storeThatNeeds.getPort());
                        if (filesToSendMap.get(file) == null) filesToSendMap.put(file, a);
                        else filesToSendMap.get(file).add(storeThatNeeds.getPort());
                    }
                } //Remove any files that were added to this send list.
                fileAdditions.put(storeThatNeeds, new ArrayList<>(fileAdditions.get(storeThatNeeds).stream().filter(x -> !inOriginalList.apply(x)).toList())); 
            }
            if (filesToSendMap.size() == 0 && fileRemoves.get(dStore).size() == 0) {
                expectedAckNum--;
                continue;
            }
            ArrayList<String> filesToSendStrings = new ArrayList<>();
            for (String file :  filesToSendMap.keySet()) {
                filesToSendStrings.add(file);
                filesToSendStrings.add("" + filesToSendMap.get(file).size());
                filesToSendStrings.addAll(filesToSendMap.get(file).stream().map(x -> "" + x).toList());
            }
            String filesToSend = filesToSendMap.size() + " " + filesToSendStrings.stream().collect(Collectors.joining(" "));
            String filesToRemove = fileRemoves.get(dStore).size() + " " + fileRemoves.get(dStore).stream().collect(Collectors.joining(" "));
            if (filesToSendMap.size() == 0) filesToSend = "0";
            if (fileRemoves.get(dStore).size() == 0) filesToRemove = "0";
            dStore.sendMessage("REBALANCE " + filesToSend + " " + filesToRemove);
        }
        expectedRebalanceAcks = new CountDownLatch(expectedAckNum);
        boolean replies = expectedRebalanceAcks.await(timeout, TimeUnit.MILLISECONDS);
        if (!replies) throw new Exception("Not all Dstores REBALANCE_ACKed " + expectedRebalanceAcks.getCount());

        Logger.info("Rebalance successful", this);
        index.updateAll(dStoreLists);
    }

    /**
     * Handle when the client sends a REMOVE message.
     * Sends a REMOVE request to each of the Dstores which have the file.
     * Will then start a new Thread to handle waiting for when all of the REMOVE_ACKs have arrived, and notifying the client when they do.
     * @param fileName Name of file being removed
     * @param client Client connection that requested the remove
     */
    public void requestRemove(String fileName, ClientConnection client) throws Exception {
            synchronized (expectedRemoveAcks) {
                checkState(fileName, "store complete", false);
                index.updateStatus(fileName, "remove in progress");
            }
            expectedRemoveAcks.put(fileName, new CountDownLatch(index.getFileDstores(fileName).size()));
            for (DstoreCon dStore : index.getFileDstores(fileName)) dStore.sendMessage("REMOVE " + fileName);
            try {if (expectedRemoveAcks.get(fileName).await(timeout, TimeUnit.MILLISECONDS)) {
                    client.sendMessage("REMOVE_COMPLETE"); 
                    index.removeFile(fileName);
            }} catch (InterruptedException e) { }
    }

    /**
     * Attempt to get a Dstore which has the requested filename
     * @param fileName Filename to be loaded
     * @param client Client which made the request
     */
    public ArrayList<DstoreCon> requestLoad(String fileName, ClientConnection client, ArrayList<DstoreCon> lastAttempt) throws Exception {
        //Wait if there is currently a rebalance ongoing
        checkState(fileName, "store complete", true);

        ArrayList<DstoreCon> dStores = index.getFileDstores(fileName);
        if (dStores == null) throw new Exception("ERROR_LOAD");
        dStores.removeAll(lastAttempt);
        if (dStores.size() == 0) throw new Exception("ERROR_LOAD");
        Integer fileSize = index.getFileSize(fileName);

        client.sendMessage("LOAD_FROM " + dStores.get(0).getPort() + " " + fileSize);
        lastAttempt.add(dStores.get(0));
        return lastAttempt;
    }

    /**
     * Handle when the client sends a STORE message.
     * Finds *replicationFactor* Dstores with the least files and sends their ports to the Client.
     * Will then start a new Thread to handle waiting for when all of the STORE_ACKs have arrived, and notifying the client when they do.
     * @param fileName Name of file being stored
     * @param fileSize Size of file being stored
     * @param client Client connection that requested the store.
     */
    public void requestStore(String fileName, Integer fileSize, ClientConnection client) throws Exception {
        //Thread to handle waiting for all of the STORE_ACKs to arrive      
        synchronized (expectedStoreAcks) {
            checkState(fileName, null, false);
            index.putFile(fileName, "store in progress", fileSize);
        }
        expectedStoreAcks.put(fileName, new CountDownLatch(replicationFactor));
        ArrayList<DstoreCon> stores = new ArrayList<>(index.getDStoreListSorted().stream().limit(replicationFactor).toList());
        client.sendMessage("STORE_TO "+ stores.stream().map(x -> Integer.toString(x.getPort())).collect(Collectors.joining(" ")));
        try {if (!expectedStoreAcks.get(fileName).await(timeout, TimeUnit.MILLISECONDS)) index.removeFile(fileName);
            else {
                client.sendMessage("STORE_COMPLETE"); 
                index.updateStatus(fileName, "store complete");
                for (DstoreCon d : stores) index.addRelation(d, fileName);
        }} catch (Exception e) {}
    }

    /**
     * Handle when a client requests a LIST
     * @param client Client that requested
     */
    public void requestList(ClientConnection client) throws Exception {
        checkState(null, null, false);

        ArrayList<String> files = new ArrayList<>(index.getFileSet().stream().filter(x -> "store complete".equals(index.getFileStatus(x))).toList());
        if (files.size() == 0) client.sendMessage("LIST");
        else client.sendMessage("LIST " + files.stream().collect(Collectors.joining(" ")));
    }

    /**
     * Determine what type of connection this socket is.
     * @param socket Socket from "listenForConections()".
     */
    private void handleNewConnection(Socket socket) {
        new Thread(() -> {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                socket.setSoTimeout(timeout);
                String currLine = "";
                while ((currLine = br.readLine()) != null) {
                    Boolean doRebal = false;
                    socket.setSoTimeout(0);
                    rebalLock.lock(); //Make things wait for a rebalance to finish before connecting
                    if (currLine.startsWith("JOIN")) {
                        DstoreCon d = new DstoreCon(socket, Integer.parseInt(currLine.substring(5)), br, this);
                        Logger.info("Dstore connected. Port: " + d.getPort(), this);
                        index.addDstore(d);
                        doRebal = true;
                    } else {
                        ClientConnection newClient = new ClientConnection(socket, "client" + clients.size(), br, this);
                        Logger.info("Client connected. client" + clients.size() + " message: " + currLine, this);
                        clients.add(newClient);
                        newClient.reveiveMessage(currLine);
                    }
                    rebalLock.unlock();
                    if (doRebal) rebalScheduler.execute(() -> rebalance());
                    break;
                }
            } catch (Exception e) { 
                Logger.err("Something went wrong with the connection", e, this); 
                rebalLock.unlock();
            }
        }, "NewConnections").start();
    }

    /**
     * Check the state of the system to see if requests can be handled
     * @param fileName name of file
     * @param status expected status of file (if null it will check that the file isn't in the index)
     * @throws Exception with the message to send to client if there's an error
     */
    private void checkState(String fileName, String status, boolean checkSize) throws Exception {
        if (index.getDstoreNum() < replicationFactor) {
            Logger.info(index.getDstoreNum() + " out of " + replicationFactor + " Dstores connected", this);
            throw new Exception("ERROR_NOT_ENOUGH_DSTORES");
        }
        if (fileName == null) return;
        if (status == null) { if (index.getFileSet().contains(fileName)) throw new Exception("ERROR_FILE_ALREADY_EXISTS"); } 
        else {
            if (checkSize && index.getFileSize(fileName) == null) throw new Exception("ERROR_FILE_DOES_NOT_EXIST");
            if (!status.equals(index.getFileStatus(fileName))) throw new Exception("ERROR_FILE_DOES_NOT_EXIST");
        }
    }

    /**
     * Handle if the socket is a Dstore connection.
     */
    private class DstoreCon extends ConnectionThread<Controller> {
        private final int port;

        public DstoreCon(Socket socket, int port, BufferedReader br, Controller controller) throws IOException {
            super(socket, "Dstore"+port, br, controller);
            this.port = port;
        }

        public void reveiveMessage(String message) {
            try {
                if (message.startsWith("STORE_ACK")) server.expectedStoreAcks.get(message.split(" ")[1]).countDown();
                if (message.startsWith("REMOVE_ACK")) server.expectedRemoveAcks.get(message.split(" ")[1]).countDown();
                if (message.startsWith("LIST")) updateDstore(message);
                if (message.equals("REBALANCE_COMPLETE")) server.expectedRebalanceAcks.countDown();
            } catch(NullPointerException e) { Logger.info("Meesage malformed  / unexpected", this); }
            catch (Exception e) { Logger.err("Something went wrong with a request:", e, this);}
        }

        private void updateDstore(String message) throws NullPointerException {
            if (expectedLists.getCount() <= 0 || dStoreLists.keySet().contains(this)) return;
            String fileString = message.substring(5);
            if (fileString.isEmpty()) synchronized (dStoreLists) { dStoreLists.put(this, new ArrayList<>()); }
            else synchronized (dStoreLists) { dStoreLists.put(this, new ArrayList<>(Arrays.asList(fileString.split(" ")))); }
            expectedLists.countDown();
        }

        public int getPort() { return port; }

        @Override
        public void close() {
            super.close();
            server.index.removeDstore(this);
        }
    }

    /**
     * Handle if the socket is a Client connection
     */
    private class ClientConnection extends ConnectionThread<Controller> {

        private final HashMap<String, ArrayList<DstoreCon>> requestedLoads = new HashMap<>();
        private final ReentrantLock lock = new ReentrantLock();
    
        public ClientConnection(Socket socket, String name, BufferedReader br, Controller controller) throws IOException {
            super(socket, name, br, controller);
        }

        public void lock() { lock.lock(); }
        public void unlock() throws IllegalMonitorStateException { lock.unlock(); }

        public void reveiveMessage(String message) {
            try {
                lock.lock();
                if (message.startsWith("LIST")) { server.requestList(this); return; }
                String fileName = message.split(" ")[1];
                if (message.startsWith("STORE")) server.requestStore(fileName, Integer.parseInt(message.split(" ")[2]), this);
                if (message.startsWith("LOAD")) requestedLoads.put(fileName, server.requestLoad(fileName, this, new ArrayList<>()));
                if (message.startsWith("RELOAD")) requestedLoads.put(fileName, server.requestLoad(fileName, this, requestedLoads.get(fileName)));
                if (message.startsWith("REMOVE")) server.requestRemove(fileName, this);
            } catch (NullPointerException e) { Logger.info("Message malformed", this); }
            catch (ArrayIndexOutOfBoundsException e) { Logger.info("Message malformed", this); }
            catch (Exception e) { 
                this.sendMessage(e.getMessage()); 
                requestedLoads.put(message.split(" ")[1], null);
            } finally { lock.unlock(); }
        }
    }

    private class FileIndex {
        private ConcurrentHashMap<String, String> fileStatus = new ConcurrentHashMap<>();
        private ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
        private ConcurrentHashMap<DstoreCon, ArrayList<String>> dStoreFiles = new ConcurrentHashMap<>(); 
        private ConcurrentHashMap<String, ArrayList<DstoreCon>> fileDstores = new ConcurrentHashMap<>();

        public synchronized void updateAll(HashMap<DstoreCon, ArrayList<String>> newIndex) {
            ConcurrentHashMap<DstoreCon, ArrayList<String>> oldStores = new ConcurrentHashMap<>(); //Dstores that were not re-listed
            dStoreFiles.forEachEntry(0, x -> { if (!newIndex.keySet().contains(x.getKey())) oldStores.entrySet().add(x); });
            dStoreFiles = new ConcurrentHashMap<>(newIndex);
            dStoreFiles.entrySet().addAll(oldStores.entrySet());
            fileDstores = new ConcurrentHashMap<>();
            for (DstoreCon dStore : newIndex.keySet()) {
                for (String file : newIndex.get(dStore)) {
                    if (fileDstores.keySet().contains(file)) fileDstores.get(file).add(dStore);
                    else {
                        ArrayList<DstoreCon> value = new ArrayList<>();
                        value.add(dStore);
                        fileDstores.put(file, value);
                    }
                }
            }
            for (String oldFile : fileStatus.keySet()) {
                if (fileDstores.containsKey(oldFile)) fileStatus.put(oldFile, "store complete");
                else removeFile(oldFile);
            }
        }

        public void addRelation(DstoreCon dStore, String fileName) throws Exception {
            ArrayList<DstoreCon> dStores;
            ArrayList<String> fileNames;
            try {
                dStores = fileDstores.get(fileName);
                fileNames = dStoreFiles.get(dStore);
                if(dStores.contains(dStore) || fileNames.contains(fileName)) throw new Exception("Relation already exists");
            } catch (NullPointerException e) {
                //The file probably doesn't exist in the table yet
                dStores = new ArrayList<>();
                fileNames = new ArrayList<>();
                fileDstores.put(fileName, dStores);
                dStoreFiles.put(dStore, fileNames);
            }
            dStores.add(dStore);
            fileNames.add(fileName);
        }

        public synchronized void removeDstore(DstoreCon dStore) {
            for (String fileName : dStoreFiles.get(dStore)) { fileDstores.get(fileName).remove(dStore); }
            dStoreFiles.remove(dStore);
        }
        public synchronized void addDstore(DstoreCon dStore) { dStoreFiles.put(dStore, new ArrayList<>()); }
                
        public void putFile(String fileName, String status, Integer size) {
            fileStatus.put(fileName, status);
            fileSizes.put(fileName, size); 
        }
        public void updateStatus(String fileName, String status) { fileStatus.put(fileName, status); }
        public void removeFile(String fileName) { 
            fileStatus.remove(fileName);
            fileSizes.remove(fileName);
            
            for (DstoreCon dStore : fileDstores.get(fileName)) { dStoreFiles.get(dStore).remove(fileName); }
            fileDstores.remove(fileName);
        }
        
        public String getFileStatus(String fileName) { return fileStatus.get(fileName); }
        public Integer getFileSize(String fileName) { return fileSizes.get(fileName); }
        public ArrayList<DstoreCon> getFileDstores(String fileName) { return fileDstores.get(fileName); }
        public ArrayList<String> getDstoreFiles(DstoreCon dStore) { return dStoreFiles.get(dStore); }

        public Set<DstoreCon> getDStoreSet() { return dStoreFiles.keySet(); }
        public ArrayList<DstoreCon> getDStoreListSorted() {
            return new ArrayList<>(index.getDStoreSet().stream().sorted(Comparator.comparing(this::getFileNumInDstore)).toList());
        }        
        public Set<String> getFileSet() { return fileStatus.keySet(); }
        public Integer getFileNumInDstore(DstoreCon dStore) { return getDstoreFiles(dStore).size(); }
        public Integer getDstoreNum() { return dStoreFiles.keySet().size(); }
    }
}