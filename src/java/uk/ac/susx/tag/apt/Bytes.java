package uk.ac.susx.tag.apt;

/**
 * @author ds300
 */
public class Bytes {
    public final byte[] backingArray;
    public final int offset;
    public final int length;

    public Bytes(byte[] backingArray, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > backingArray.length) throw new IllegalArgumentException();
        this.backingArray = backingArray;
        this.offset = offset;
        this.length = length;
    }

    public Bytes(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public Bytes(byte[] bytes, int length) {
        this(bytes, 0, length);
    }

    public byte[] toByteArray() {
        byte[] result = new byte[length];
        System.arraycopy(backingArray, offset, result, 0, length);
        return result;
    }
}
