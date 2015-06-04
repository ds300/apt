package uk.ac.susx.tag.apt;

import java.util.Arrays;

/**
 * Created by ds300 on 04/06/2015.
 */
public class AdditionComposer extends EdgeResolutionComposer {
    public AdditionComposer() {
        super(new NodeAlignmentEdgeResolver(new NodeIntersectionCalculator() {
            @Override
            public Int2FloatArraySortedMap intersect(Int2FloatArraySortedMap a, Int2FloatArraySortedMap b) {
                int[] keys = new int[a.keys.length + b.keys.length];
                float[] vals = new float[a.keys.length + b.keys.length];

                int ai = 0, bi = 0, i = 0;
                while (ai < a.keys.length && bi < b.keys.length) {
                    if (a.keys[ai] == b.keys[bi]) {
                        keys[i] = keys[ai];
                        vals[i] = a.vals[ai] + b.vals[bi];

                        ai++;
                        bi++;
                    } else if (a.keys[ai] < b.keys[bi]) {
                        keys[i] = a.keys[ai];
                        vals[i] = a.vals[ai];

                        ai++;
                    } else {
                        keys[i] = b.keys[bi];
                        vals[i] = b.vals[bi];

                        bi++;
                    }
                    i++;
                }

                while (ai < a.keys.length) {
                    keys[i] = a.keys[ai];
                    vals[i] = a.vals[ai];

                    ai++;
                    i++;
                }

                while (bi < b.keys.length) {
                    keys[i] = b.keys[bi];
                    vals[i] = b.vals[bi];

                    bi++;
                    i++;
                }

                return new Int2FloatArraySortedMap(Arrays.copyOf(keys, i), Arrays.copyOf(vals, i));

            }
        }), Direction.BOTTOM_UP);
    }
}
