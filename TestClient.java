
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

class TestClient {
    public static void main(String[] args) {
        TestClient logClient = new TestClient();

        try {
            Socket socket = new Socket("localhost", 12345);
            PrintWriter controllerWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            controllerWriter.println("a");            
            
            new Thread(() -> {
                String currLine;
                Logger.info("Listening to controlelr", logClient);
                try {
                    while ((currLine = br.readLine()) != null) {
                        if (currLine.isEmpty()) continue;
                        Logger.info("Mesasge recieved: " + currLine, logClient);
                    }
                    br.close();
                    socket.close();
                } catch (Exception e) {
                    Logger.info("Exception in the message recieve", logClient);
                }
            }, "Recieve message").start();

            InputStreamReader sysIn = new InputStreamReader(System.in);
            BufferedReader reader = new BufferedReader(sysIn);
            String currLine;

            for (String file : new String[] {"a","b","c"}) {
                controllerWriter.println("STORE " + file + " 5");
                Thread.sleep(100);

                Socket os1 = new Socket("localhost", 1234);
                PrintWriter pw1 = new PrintWriter(new OutputStreamWriter(os1.getOutputStream()), true);
                pw1.println("STORE " + file + " 5");
                os1.getOutputStream().write("aaaaa".getBytes());
                os1.close();

                Socket os2 = new Socket("localhost", 1235);
                PrintWriter pw2 = new PrintWriter(new OutputStreamWriter(os2.getOutputStream()), true);
                pw2.println("STORE " + file + " 5");
                os2.getOutputStream().write("aaaaa".getBytes());
                os2.close();
                Thread.sleep(100);

                Socket os3 = new Socket("localhost", 1236);
                PrintWriter pw3 = new PrintWriter(new OutputStreamWriter(os3.getOutputStream()), true);
                pw3.println("STORE " + file + " 5");
                os3.getOutputStream().write("aaaaa".getBytes());
                os3.close();
                Thread.sleep(100);
            }

            Logger.info("Ready to revieve input", logClient);
            Socket dstore = null;
            while ((currLine = reader.readLine()) != null) {
                try {
                    int port = Integer.parseInt(currLine.split(" ")[0]);
                    dstore = new Socket("localhost", port);
                    Logger.info("Trying to connect to port: " + port, logClient);
                    PrintWriter pw = new PrintWriter(new OutputStreamWriter(dstore.getOutputStream()), true);
                    pw.println(currLine.split(" ",2)[1]);
                    Logger.info("Sent message", logClient);
                    var chars = new char[5];
                    sysIn.read(chars, 0, 5);
                    Logger.info("Read", logClient);
                    Charset charset = Charset.forName("UTF-8");
                    ByteBuffer byteBuffer = charset.encode(CharBuffer.wrap(chars));
                    byte[] bytes = Arrays.copyOf(byteBuffer.array(), byteBuffer.limit());
                    dstore.getOutputStream().write(bytes);
                    continue;
                } catch (NullPointerException e) {
                    Logger.info("Sending to controller:", logClient);
                } catch (Exception e) {
                    Logger.info("Dstore closed", logClient);
                    if (dstore != null) dstore.close();
                }
                controllerWriter.println(currLine);
            }
        } catch (Exception e) {
            Logger.err("Something went wrong", e, logClient);
        }

    }
}