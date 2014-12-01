package uk.ac.susx.tag.apt;

import java.io.IOException;

/**
 * @author ds300
 */
public interface APTComposer<T extends APT> {
    T[] compose(DistributionalLexicon<?, ?, APT> lexicon, RGraph graph) throws IOException;
}
