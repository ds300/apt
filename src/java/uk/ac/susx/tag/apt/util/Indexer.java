package uk.ac.susx.tag.apt.util;

import java.util.Map;

/**
 * Created by ds300 on 14/09/2015.
 */
public class Indexer extends AbstractIndexer {

    private Indexer(BidirectionalIndex index) {
        setState(index);
    }

    private Indexer() {}

    public static Indexer from(Map<String, Integer> val2idx) {
        return new Indexer(BidirectionalIndex.from(val2idx));
    }

    public static Indexer empty() {
        return new Indexer();
    }

    @Override
    public boolean hasIndex(String value) {
        return getState().val2idx.containsKey(value);
    }

    @Override
    public String resolve(int index) {
        return (String) getState().idx2val.valAt(index);
    }

}
