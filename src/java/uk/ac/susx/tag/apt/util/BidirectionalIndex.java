package uk.ac.susx.tag.apt.util;


import clojure.lang.APersistentMap;
import clojure.lang.IPersistentMap;
import clojure.lang.ITransientMap;
import clojure.lang.PersistentHashMap;

import java.util.Map;

/**
 * Created by ds300 on 11/09/2015.
 */
public class BidirectionalIndex {
    public final IPersistentMap idx2val;
    public final IPersistentMap val2idx;
    public final int nextIdx;

    BidirectionalIndex(IPersistentMap idx2val,
                              IPersistentMap val2idx,
                              int nextIdx) {
        this.idx2val = idx2val;
        this.val2idx = val2idx;
        this.nextIdx = nextIdx;
    }

    BidirectionalIndex with(String val) {
        return new BidirectionalIndex(
                idx2val.assoc(nextIdx, val),
                val2idx.assoc(val, nextIdx),
                nextIdx + 1);
    }

    static final BidirectionalIndex EMPTY = new BidirectionalIndex(
            PersistentHashMap.EMPTY,
            PersistentHashMap.EMPTY,
            0);

    public static BidirectionalIndex from(Map<String, Integer> val2idx) {
        IPersistentMap _val2idx = PersistentHashMap.create(val2idx);
        ITransientMap _idx2val = PersistentHashMap.EMPTY.asTransient();
        int maxIdx = 0;
        for (Map.Entry<String, Integer> e : val2idx.entrySet()) {
            String val = e.getKey();
            int idx = e.getValue();
            if (idx > maxIdx) {
                maxIdx = idx;
            }
            _idx2val = _idx2val.assoc(idx, val);
        }
        return new BidirectionalIndex(_val2idx, _idx2val.persistent(), maxIdx + 1);
    }
}
