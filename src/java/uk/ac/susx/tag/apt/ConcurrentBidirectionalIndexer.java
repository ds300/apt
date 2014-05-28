package uk.ac.susx.tag.apt;

import clojure.lang.APersistentMap;
import clojure.lang.PersistentHashMap;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ds300
 */
public class ConcurrentBidirectionalIndexer<T> implements BidirectionalIndexer<T> {
    private AtomicReference<APersistentMap> value2Index = new AtomicReference<APersistentMap>(PersistentHashMap.EMPTY);
    private AtomicReference<APersistentMap> index2Value = new AtomicReference<APersistentMap>(PersistentHashMap.EMPTY);

    public int getIndex(T val) {
        APersistentMap currentIndex = value2Index.get();
        if (currentIndex.containsKey(val)) {
            return (Integer) currentIndex.get(val);
        } else {
            for (;;) {
                int featureIdx = currentIndex.size();
                APersistentMap newIndex = (APersistentMap) currentIndex.assoc(val, featureIdx);
                if (value2Index.compareAndSet(currentIndex, newIndex)) {
                    currentIndex = index2Value.get();
                    for (;;) {
                        newIndex = (APersistentMap) currentIndex.assoc(featureIdx, val);
                        if (index2Value.compareAndSet(currentIndex, newIndex)) {
                            return featureIdx;
                        } else {
                            currentIndex = index2Value.get();
                        }
                    }
                } else {
                    currentIndex = value2Index.get();
                }
            }
        }
    }

    @Override
    public boolean hasIndex(T feature) {
        return value2Index.get().containsKey(feature);
    }

    @Override
    public T resolve(int featureIndex) {
        return (T) index2Value.get().valAt(featureIndex);
    }
}
