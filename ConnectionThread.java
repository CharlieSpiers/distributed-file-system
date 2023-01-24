import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

abstract class ConnectionThread<E> extends Thread implements Closeable {
        protected final Socket socket;
        protected final BufferedReader bufferedReader;
        protected final PrintWriter printWriter;
        protected final E server;

        public ConnectionThread(Socket socket, String name, E e) throws IOException {
            super(name);
            Logger.info("Creating new thread: " + name, this);
            this.socket = socket;
            this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.printWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            this.server = e;
            this.start();
        }

        public ConnectionThread(Socket socket, String name, BufferedReader br, E e) throws IOException {
            super(name);
            this.socket = socket;
            this.bufferedReader = br;
            this.printWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            this.server = e;
            this.start();
        }

        @Override
        public void run() {
            String currLine = "";
            Logger.info("Thread started successfully", this);
            try {
                while ((currLine = bufferedReader.readLine()) != null) {
                    Logger.info("Message reveived: " + currLine, this);
                    reveiveMessage(currLine);
                }
                //If the reader notices that the connection has closed
                close();
            } catch (IOException e) { close(); }
        }

        abstract void reveiveMessage(String message);

        public void sendMessage(String message) {
            Logger.info("Sending message: " + message, this);
            printWriter.println(message); 
        }

        @Override
        public void close() {
            Logger.info("Closing socket", this);
            try {
                printWriter.close();
                bufferedReader.close();
                socket.close();
            } catch (Exception e) { Logger.err("Problem closing the streams", e, this); }
        }
    }