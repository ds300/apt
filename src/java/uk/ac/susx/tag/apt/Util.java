package uk.ac.susx.tag.apt;


import pl.edu.icm.jlargearrays.ByteLargeArray;

import java.io.ByteArrayOutputStream;
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

    public static ByteLargeArray float2largeBytes(float value) {
        return int2largeBytes(Float.floatToRawIntBits(value));
    }

    public static void float2bytes (float value, byte[] bytes, int offset) {
        int2bytes(Float.floatToRawIntBits(value), bytes, offset);
    }

    public static void float2largeBytes(float value, ByteLargeArray bytes, long offset) {
        int2largeBytes(Float.floatToRawIntBits(value), bytes, offset);
    }

    public static float bytes2float (byte[] bytes) {
        return Float.intBitsToFloat(bytes2int(bytes));
    }

    public static float largeBytes2float(ByteLargeArray bytes) {
        return Float.intBitsToFloat(largeBytes2int(bytes));
    }

    public static float bytes2float (byte[] bytes, int offset) {
        return Float.intBitsToFloat(bytes2int(bytes, offset));
    }

    public static float largeBytes2float(ByteLargeArray bytes, long offset) {
        return Float.intBitsToFloat(largeBytes2int(bytes, offset));
    }

    public static byte[] int2bytes (int i) {
        byte[] bytes = new byte[4];
        int2bytes(i, bytes, 0);
        return bytes;
    }

    public static ByteLargeArray int2largeBytes(int i) {
        ByteLargeArray bytes = new ByteLargeArray(4);
        int2largeBytes(i, bytes, 0);
        return bytes;
    }

    public static void int2bytes (int i, byte[] bytes, int offset) {
        bytes[offset + 0] = (byte)(i >>> 24);
        bytes[offset + 1] = (byte)(i >>> 16);
        bytes[offset + 2] = (byte)(i >>> 8);
        bytes[offset + 3] = (byte) i;
    }

    public static void int2largeBytes(int i, ByteLargeArray bytes, long offset) {
        bytes.setByte(offset, (byte)(i >>> 24));
        bytes.setByte(offset + 1, (byte)(i >>> 16));
        bytes.setByte(offset + 2, (byte)(i >>> 8));
        bytes.setByte(offset + 3, (byte) i);
    }

    public static int bytes2int(byte[] bytes) {
        return bytes2int(bytes, 0);
    }

    public static int largeBytes2int(ByteLargeArray bytes) {
        return largeBytes2int(bytes, 0);
    }

    public static int bytes2int(byte[] bytes, int offset) {
        int result = bytes[offset + 3]&0xFF;
        result = result ^ ((bytes[offset + 2]&0xff) << 8);
        result = result ^ ((bytes[offset + 1]&0xff) << 16);
        result = result ^ ((bytes[offset + 0]&0xff) << 24);
        return result;
    }

    public static int largeBytes2int(ByteLargeArray bytes, long offset) {
        int result = bytes.getByte(offset + 3)&0xFF;

        result = result ^ ((bytes.getByte(offset + 2)&0xff) << 8);
        result = result ^ ((bytes.getByte(offset + 1)&0xff) << 16);
        result = result ^ ((bytes.getByte(offset)&0xff) << 24);

        return result;
    }


    public static byte[] long2bytes (long i) {
        byte[] bytes = new byte[8];
        long2bytes(i, bytes, 0);
        return bytes;
    }

    public static ByteLargeArray long2largeBytes(long i) {
        ByteLargeArray bytes = new ByteLargeArray(8);
        long2largeBytes(i, bytes, 0);
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

    public static void long2largeBytes(long i, ByteLargeArray bytes, long offset) {
        bytes.setByte(offset, (byte)(i >>> 56));
        bytes.setByte(offset + 1, (byte)(i >>> 48));
        bytes.setByte(offset + 2, (byte)(i >>> 40));
        bytes.setByte(offset + 3, (byte)(i >>> 32));
        bytes.setByte(offset + 4, (byte)(i >>> 24));
        bytes.setByte(offset + 5, (byte)(i >>> 16));
        bytes.setByte(offset + 6, (byte)(i >>> 8));
        bytes.setByte(offset + 7, (byte)i);
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

    public static long largeBytes2long(ByteLargeArray bytes, long offset) {
        long result = bytes.get(offset + 7)&0xFFl;
        result = result ^ ((bytes.get(offset + 6)&0xffl) << 8);
        result = result ^ ((bytes.get(offset + 5)&0xffl) << 16);
        result = result ^ ((bytes.get(offset + 4)&0xffl) << 24);
        result = result ^ ((bytes.get(offset + 3)&0xffl) << 32);
        result = result ^ ((bytes.get(offset + 2)&0xffl) << 40);
        result = result ^ ((bytes.get(offset + 1)&0xffl) << 48);
        result = result ^ ((bytes.get(offset + 0)&0xffl) << 56);
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



    private static final char[] b64table = new char[] {
            'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'
            ,'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'
            ,'0','1','2','3','4','5','6','7','8','9'
            ,'+','/'
    };

    private static final byte[] b64reverseTable = new byte[123];

    static {
        for (int i = 0; i < b64table.length; i++) {
            b64reverseTable[b64table[i]] = (byte) i;
        }
    }

    public static String base64encode(byte[] bytes) {
        int current = 0;
        int state = 0;

        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            switch (state) {
                case 0:
                    sb.append(b64table[(b & 0b11111100) >>> 2]);
                    current = (b & 0b00000011) << 4;
                    state = 1;
                    break;
                case 1:
                    current = current ^ ((b & 0b11110000) >>> 4);
                    sb.append(b64table[current]);
                    current = (b & 0b00001111) << 2;
                    state = 2;
                    break;
                case 2:
                    current = current ^ ((b & 0b11000000) >>> 6);
                    sb.append(b64table[current]);
                    sb.append(b64table[b & 0b00111111]);
                    state = 0;
                    break;
            }
        }

        // explicit padding unnecessary
        if (state > 0) {
            sb.append(b64table[current]);
        }

        return sb.toString();
    }

    public static byte[] base64decode (String string) {
        int len = string.length();
        int iter = len - (len % 4);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int i=0;
        for (; i<iter; i+=4) {
            byte a = b64reverseTable[string.charAt(i)];
            byte b = b64reverseTable[string.charAt(i+1)];
            byte c = b64reverseTable[string.charAt(i+2)];
            byte d = b64reverseTable[string.charAt(i+3)];

            byte[] bytes = new byte[3];
            bytes[0] = (byte) ((a << 2) ^ ((b & 0b00110000) >>> 4));
            bytes[1] = (byte) (((b & 0b00001111) << 4) ^ ((c & 0b00111100) >>> 2));
            bytes[2] = (byte) (((c & 0b00000011) << 6) ^ d);

            try {
                buffer.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        switch (len - iter) {
            case 0:
                break;
            case 3: {
                byte a = b64reverseTable[string.charAt(i)];
                byte b = b64reverseTable[string.charAt(i+1)];
                byte c = b64reverseTable[string.charAt(i+2)];
                byte[] bytes = new byte[2];
                bytes[0] = (byte) ((a << 2) ^ ((b & 0b00110000) >>> 4));
                bytes[1] = (byte) (((b & 0b00001111) << 4) ^ ((c & 0b00111100) >>> 2));
                try {
                    buffer.write(bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }

            case 2: {
                byte a = b64reverseTable[string.charAt(i)];
                byte b = b64reverseTable[string.charAt(i+1)];
                byte[] bytes = new byte[1];
                bytes[0] = (byte) ((a << 2) ^ ((b & 0b00110000) >>> 4));
                try {
                    buffer.write(bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            default:
                throw new RuntimeException("Bad input string format");
        }

        return buffer.toByteArray();
    }
}
