package uk.ac.susx.tag.apt.util;


import clojure.lang.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    public static BidirectionalIndex from(IPersistentMap val2idx) {
        PersistentHashMap _val2idx = val2idx instanceof PersistentHashMap ? (PersistentHashMap) val2idx : PersistentHashMap.create(val2idx);
        AtomicInteger maxIdx = new AtomicInteger(0);

        ITransientMap _idx2val = (ITransientMap) _val2idx.kvreduce(new AFn() {
            @Override
            public Object invoke(Object arg1, Object arg2, Object arg3) {
                Integer idx = (Integer) arg3;
                String val = (String) arg2;
                ITransientMap idx2val = (ITransientMap) arg1;

                if (idx > maxIdx.get()) {
                    maxIdx.set(idx);
                }

                return idx2val.assoc(idx, val);
            }
        }, PersistentHashMap.EMPTY.asTransient());

        return new BidirectionalIndex(_idx2val.persistent(), _val2idx, maxIdx.get() + 1);
    }
}
