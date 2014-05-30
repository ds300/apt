package uk.ac.susx.tag.apt.construct;


import java.io.IOException;
import java.io.OutputStream;

/**
 * @author ds300
 */
public class RLEOutputStream extends OutputStream {
    private boolean open = true;
    private final OutputStream out;
    private final byte[] inBuf = new byte[4048];
    private final byte[] outBuf = new byte[8096];
    private int count = 0;

    public RLEOutputStream(OutputStream out) {
        this.out = out;
    }


    @Override
    public void write(int b) throws IOException {
        if (!open) throw new IOException("Attempting to write to closed stream");
        if (count == inBuf.length) flush();
        inBuf[count++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
        if (!open) throw new IOException("Attempting to write to closed stream");
        if (length < 0 || offset < 0 || offset + length > b.length)
            throw new IndexOutOfBoundsException("b.length: " + b.length
                                              + ", offset: " + offset
                                              + ", length: " + length);

        flush();
        while (length > 0) {
            int toWrite = Math.min(length, inBuf.length);
            encodeAndWrite(b, offset, toWrite);
            length -= toWrite;
            offset += toWrite;
        }

    }

    @Override
    public void write(byte[] buf) throws IOException {
        write(buf, 0, buf.length);
    }

    private void encodeAndWrite(final byte[] buf, int offset, final int length) throws IOException {
        if (length > inBuf.length) throw new IllegalArgumentException("length must be <= " + inBuf.length);
        final int target = offset + length;
        int i = 0;
        while (offset < target) {
            byte b = buf[offset++];
            outBuf[i++] = b;
            if ((b == 0 || b == -1) && offset < target) {
                byte num = 0;
                byte c = buf[offset++];
                while (c == b && offset < target && num < 126) {
                    num++;
                    c = buf[offset++];
                }
                outBuf[i++] = num;
                offset--;
            }
        }
        out.write(outBuf, 0, i);
    }

    @Override
    public void flush() throws IOException {
        if (!open) throw new IOException("Attempting to flush closed stream");
        if (count != 0) encodeAndWrite(inBuf, 0, count);
        count = 0;
        out.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        out.close();
        open = false;
    }
}
