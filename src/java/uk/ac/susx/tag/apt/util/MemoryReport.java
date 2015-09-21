package uk.ac.susx.tag.apt.util;

/**
 * Created by ds300 on 21/09/2015.
 */
public class MemoryReport {

    public final Memory max;
    public final Memory used;
    public final Memory remaining;
    public final long queryTime;

    private MemoryReport(long queryTime) {
        Runtime rt = Runtime.getRuntime();
        max = new Memory(rt.maxMemory());
        used = new Memory(rt.totalMemory() - rt.freeMemory());
        remaining = new Memory(max.bytes - used.bytes);
        this.queryTime = queryTime;
    }

    private static MemoryReport lastReport = new MemoryReport(System.currentTimeMillis());

    public static MemoryReport get() {
        long currentTime = System.currentTimeMillis();
        if (currentTime >= lastReport.queryTime + 1000) {
            lastReport = new MemoryReport(currentTime);
        }
        return lastReport;
    }

    private static double decimalPlaces(double n, int dp) {
        return ((int) (n * (Math.pow(10, dp)))) / Math.pow(10, dp);
    }

    public static class Memory {
        public final long bytes;

        public Memory(long bytes) {
            this.bytes = bytes;
        }

        public double asGigabytes(int dp) {
            return decimalPlaces(((double) bytes) / (1024 * 1024 * 1024), dp);
        }

        public double asMegabytes(int dp) {
            return decimalPlaces(((double) bytes) / (1024 * 1024), dp);
        }

        public double asKilobytes(int dp) {
            return decimalPlaces(((double) bytes) / 1024, dp);
        }

        public String humanReadable() {
            if (bytes < 1024) {
                return bytes + "B";
            } else if (bytes < (1024 * 1024)) {
                return asKilobytes(1) + "kB";
            } else if (bytes < (1024 * 1024 * 1024)) {
                return asMegabytes(1) + "MB";
            } else {
                return asGigabytes(1) + "GB";
            }
        }
    }
}
