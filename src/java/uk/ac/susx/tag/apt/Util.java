package uk.ac.susx.tag.apt;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author ds300
 * conversions from bytes assume big endian, conversions to bytes output big endian.
 */
public class Util {
    public static byte [] float2Bytes (float value)
    {
        return int2bytes(Float.floatToRawIntBits(value));
    }

    public static void float2bytes (float value, byte[] bytes, int offset) {
        int2bytes(Float.floatToRawIntBits(value), bytes, offset);
    }

    public static float bytes2float (byte[] bytes) {
        return Float.intBitsToFloat(bytes2int(bytes));
    }

    public static float bytes2float (byte[] bytes, int offset) {
        return Float.intBitsToFloat(bytes2int(bytes, offset));
    }

    public static byte[] int2bytes (int i) {
        byte[] bytes = new byte[4];
        int2bytes(i, bytes, 0);
        return bytes;
    }


    public static void int2bytes (int i, byte[] bytes, int offset) {
        bytes[offset + 0] = (byte)(i >>> 24);
        bytes[offset + 1] = (byte)(i >>> 16);
        bytes[offset + 2] = (byte)(i >>> 8);
        bytes[offset + 3] = (byte) i;
    }

    public static int bytes2int(byte[] bytes) {
        return bytes2int(bytes, 0);
    }

    public static int bytes2int(byte[] bytes, int offset) {
        int result = bytes[offset + 3]&0xFF;
        result = result ^ ((bytes[offset + 2]&0xff) << 8);
        result = result ^ ((bytes[offset + 1]&0xff) << 16);
        result = result ^ ((bytes[offset + 0]&0xff) << 24);
        return result;
    }


    public static byte[] long2bytes (long i) {
        byte[] bytes = new byte[8];
        long2bytes(i, bytes, 0);
        return bytes;
    }

    public static void long2bytes (long i, byte[] bytes, int offset) {
        bytes[offset] = (byte)(i >>> 56);
        bytes[offset + 1] = (byte)(i >>> 48);
        bytes[offset + 2] = (byte)(i >>> 40);
        bytes[offset + 3] = (byte)(i >>> 32);
        bytes[offset + 4] = (byte)(i >>> 24);
        bytes[offset + 5] = (byte)(i >>> 16);
        bytes[offset + 6] = (byte)(i >>> 8);
        bytes[offset + 7] = (byte)i;
    }
    public static long bytes2long(byte[] bytes) {
        return bytes2long(bytes, 0);
    }

    public static long bytes2long(byte[] bytes, int offset) {
        long result = bytes[offset + 7]&0xFFl;
        result = result ^ ((bytes[offset + 6]&0xffl) << 8);
        result = result ^ ((bytes[offset + 5]&0xffl) << 16);
        result = result ^ ((bytes[offset + 4]&0xffl) << 24);
        result = result ^ ((bytes[offset + 3]&0xffl) << 32);
        result = result ^ ((bytes[offset + 2]&0xffl) << 40);
        result = result ^ ((bytes[offset + 1]&0xffl) << 48);
        result = result ^ ((bytes[offset + 0]&0xffl) << 56);
        return result;
    }

    /**
     * expects sorted arrays with no duplicates
     * @param a
     * @param b
     * @return the number of unique numbers in the arrays
     */
    public static int countUnique(int[] a, int[] b) {
        int uniques = 0;
        int i=0, j=0;
        for (; i < a.length && j < b.length;) {
            uniques++;
            if (a[i] == b[j]) {
                i++; j++;
            } else if (a[i] < b[j]) {
                i++;
            } else {
                j++;
            }
        }
        uniques += a.length - i;
        uniques += b.length - j;
        return uniques;
    }

    /**
     * expects sorted arrays with no duplicates
     * @param a
     * @param b
     * @return the number of shared numbers in the arrays
     */
    public static int countShared(final int[] a, final int[] b) {
        int shared = 0;
        int i=0, j=0;
        for (; i < a.length && j < b.length;) {
            int ai = a[i];
            int bj = b[j];
            if (ai == bj) {
                shared++;
                i++;
                j++;
            } else if (ai < bj) {
                i++;
            } else {
                j++;
            }
        }
        return shared;
    }

    public static int read(final InputStream inputStream, final byte[] buf, final int offset, final int length) throws IOException {
        int bytesRead = 0;
        int n = 0;
        while (true) {
            n = inputStream.read(buf, offset + bytesRead, length - bytesRead);
            if (n > -1) {
                bytesRead += n;
                if (bytesRead >= length) {
                    return bytesRead;
                }
            } else {
                return bytesRead;
            }
        }
    }
}
