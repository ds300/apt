package uk.ac.susx.tag.apt.util;

import clojure.lang.*;

import java.util.Map;

/**
 * Created by ds300 on 11/09/2015.
 */
public class RelationIndexer extends AbstractIndexer {

    private RelationIndexer(BidirectionalIndex index) {
        setState(index);
    }

    public static RelationIndexer from(IPersistentMap val2idx) {
        return new RelationIndexer(BidirectionalIndex.from(val2idx));
    }

    public static RelationIndexer empty() {
        return new RelationIndexer(new BidirectionalIndex(PersistentHashMap.EMPTY, PersistentHashMap.EMPTY, 1));
    }


    @Override
    public int getIndex(String value) {
        if (value.startsWith("_")) {
            return -super.getIndex(value.substring(1));
        } else {
            return super.getIndex(value);
        }
    }

    @Override
    public boolean hasIndex(String value) {
        if (value.startsWith("_")) {
            return hasIndex(value.substring(1));
        } else {
            return getState().val2idx.containsKey(value);
        }
    }


    @Override
    public String resolve(int idx) {
        if (idx < 0) {
            String val = (String) getState().idx2val.valAt(-idx);
            if (val != null) {
                return "_" + val;
            } else {
                return null;
            }
        } else {
            return (String) getState().idx2val.valAt(idx);
        }
    }

}
