package uk.ac.susx.tag.apt;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author ds300
 */
public interface Writable {
    void writeTo(OutputStream out) throws IOException;
}

