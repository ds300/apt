package uk.ac.susx.tag.apt.util;

import clojure.lang.RT;
import uk.ac.susx.tag.apt.BidirectionalIndexer;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by ds300 on 14/09/2015.
 */
public abstract class AbstractIndexer implements BidirectionalIndexer<String> {

    private final AtomicReference<BidirectionalIndex> state = new AtomicReference<>(BidirectionalIndex.EMPTY);
    public final BidirectionalIndex getState () {
        return state.get();
    }
    protected final void setState(BidirectionalIndex index) {
        state.set(index);
    }

    @Override
    public final Iterable<String> getValues() {
        return (Iterable<String>) RT.keys(getState().val2idx);
    }

    @Override
    public final Iterable<Integer> getIndices() {
        return (Iterable<Integer>) RT.keys(getState().idx2val);
    }

    @Override
    public int getIndex(String value) {
        BidirectionalIndex index = getState();
        Integer idx = (Integer) index.val2idx.valAt(value);

        if (idx == null) {
            synchronized (this) {
                BidirectionalIndex currentIndex = getState();
                if (currentIndex == index || (idx = (Integer) index.val2idx.valAt(value)) == null) {
                    // idx still null, no need to lookup again
                    BidirectionalIndex newIndex = index.with(value);
                    setState(newIndex);

                    idx = newIndex.nextIdx - 1;
                }
            }
        }
        return idx;
    }
}
