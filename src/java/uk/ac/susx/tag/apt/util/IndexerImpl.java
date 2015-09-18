package uk.ac.susx.tag.apt.util;

import java.util.Map;

/**
 * Created by ds300 on 14/09/2015.
 */
public class IndexerImpl extends AbstractIndexer {

    private IndexerImpl(BidirectionalIndex index) {
        setState(index);
    }

    private IndexerImpl() {}

    public static IndexerImpl from(Map<String, Integer> val2idx) {
        return new IndexerImpl(BidirectionalIndex.from(val2idx));
    }

    public static IndexerImpl empty() {
        return new IndexerImpl();
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
