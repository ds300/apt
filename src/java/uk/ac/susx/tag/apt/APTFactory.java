package uk.ac.susx.tag.apt;

import java.io.IOException;
import java.io.InputStream;

/**
 * For creating the APTs
 * @author ds300
 */
public interface APTFactory<T extends APT> {
    /**
     * Read and return an APT from the given InputStream
     * @param inputStream the InputStream
     * @return an APT
     */
    T read(InputStream inputStream) throws IOException;

    /**
     * Construct an empty APT
     * @return an empty APT
     */
    T empty();

    /**
     * Create an array of APTs from a graph
     * @param graph
     * @return
     */
    T[] fromGraph(RGraph graph);
}

