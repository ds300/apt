package uk.ac.susx.tag.apt;

/**
 * @author ds300
 */
public interface APTComposer<T extends APT> {
    T[] compose(DistributionalLexicon<?, ?, APT> lexicon, RGraph graph);
}
