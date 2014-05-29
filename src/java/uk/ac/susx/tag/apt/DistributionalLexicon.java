package uk.ac.susx.tag.apt;

import java.io.IOException;

/**
 * @author ds300
 */
public interface DistributionalLexicon<T extends APT> {
    T getAPT(int entityId) throws IOException;
    void include(int entityId, APT apt) throws IOException;
    void replace(int entityId, APT apt) throws IOException;
    void remove(int entityId) throws IOException;

}
