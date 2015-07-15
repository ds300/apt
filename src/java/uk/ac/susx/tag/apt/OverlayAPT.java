package uk.ac.susx.tag.apt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;

/**
 * Created by ds300 on 15/07/2015.
 */
public class OverlayAPT {
    public static class ArrayAPTPathTuple {
        public final ArrayAPT apt;
        public final int[] path;

        public ArrayAPTPathTuple(ArrayAPT apt, int[] path) {
            this.apt = apt;
            this.path = path;
        }
    }
    public final ArrayList<ArrayAPTPathTuple> overlaidNodes = new ArrayList<>();
    public final HashMap<Integer, OverlayAPT> edges = new HashMap<>();

    public void overlay(ArrayAPT apt) {
        overlay(apt, Integer.MAX_VALUE);
    }

    public void overlay(ArrayAPT apt, int depth) {
        overlay(apt, new int[]{}, 0, depth);
    }

    private void overlay(ArrayAPT apt, int[] path, int returnPath, int depth) {
        overlaidNodes.add(new ArrayAPTPathTuple(apt, path));
        if (depth > 0) {
            for (int edge : apt.edges) {
                if (edge != returnPath) {
                    int[] newPath = new int[path.length + 1];
                    System.arraycopy(path, 0, newPath, 0, path.length);
                    newPath[path.length] = edge;
                    getOrMakeEdge(edge).overlay(apt.getChild(edge), newPath, -edge, depth - 1);
                }
            }
        }
    }

    public OverlayAPT getOrMakeEdge(int relation) {
        OverlayAPT child = edges.get(relation);
        if (child == null) {
            child = new OverlayAPT();
            edges.put(relation, child);
        }
        return child;
    }

    public OverlayAPT getOrMakeIn(int[] path) {
        OverlayAPT result = this;
        for (int i : path) result = result.getOrMakeEdge(i);
        return result;
    }

    public ArrayAPT collapse(Function<ArrayList<ArrayAPTPathTuple>, ArrayAPT> collapser) {
        return collapse(collapser, 0);
    }

    private ArrayAPT collapse(Function<ArrayList<ArrayAPTPathTuple>, ArrayAPT> collapser, int returnPath) {
        ArrayAPT acc = collapser.apply(overlaidNodes);

        for (int edge : edges.keySet()) {
            if (edge != returnPath) {
                acc = acc.withEdge(edge, edges.get(edge).collapse(collapser, -edge));
            }
        }

        return acc;
    }
}
