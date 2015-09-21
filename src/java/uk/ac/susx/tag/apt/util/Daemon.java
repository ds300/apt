package uk.ac.susx.tag.apt.util;

/**
 * Created by ds300 on 21/09/2015.
 */
public class Daemon {
    private volatile boolean running = false;
    private volatile boolean sleeping = false;
    private Thread daemonThread = new Thread() {
        @Override
        public void run () {
            while (running) {
                task.run();
                System.out.println("yo i also running bro");
                try {
                    sleeping = true;
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ignored) {
                    running = false;
                } finally {
                    sleeping = false;
                }
            }
        }
    };

    public final Runnable task;
    public final long sleepTime;

    public Daemon(Runnable task, long sleepTime) {
        this.task = task;
        this.sleepTime = sleepTime;
    }

    public void start() {
        System.out.println("me running bro");
        running = true;
        daemonThread.start();
    }

    public void stop() {
        running = false;
        if (sleeping) {
            daemonThread.interrupt();
        }
    }
}
