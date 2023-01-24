import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

class Logger {
    public static void clearLogs() {
        try {
            FileWriter clearFile = new FileWriter("log.txt");
            clearFile.write("");
            clearFile.close();
        } catch (Exception e) {
            System.err.println("Couldn't clear log file");
        }
    }

    private static void log(String log) {
        try {
            FileWriter fw = new FileWriter("log.txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.append(log + "\n");
            bw.close();
            fw.close();
        } catch (Exception e) {
            System.err.println("Couldn't write to  log file");
        }
    }

    public static void info(String info, Object clazz) {
        Thread thread = Thread.currentThread();
        String threadInfo = thread.getName() + ":" + thread.getId();
        String message = getTime() + " | " + clazz.getClass().getCanonicalName() + " | " + threadInfo + " | " + info;
        log(message);
        System.out.println(message);
    }

    public static void err(String info, Exception exception, Object clazz) {
        Thread thread = Thread.currentThread();
        String threadInfo = thread.getName() + ":" + thread.getId();
        String message = getTime() + " | " + clazz.getClass().toString().substring(6) + " | " + threadInfo + " | " + info;
        exception.printStackTrace();
        log(message);
        System.err.println(message);
    }

    private static String getTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        return dtf.format(LocalDateTime.now());
    }
}