package uk.ac.susx.tag.apt;

import java.util.Arrays;

/**
 * @author ds300
 */
public class BinaryIntersectionComposer implements APTComposer<ArrayAPT> {

    public static enum Direction {
        TOP_DOWN, BOTTOM_UP
    }

    public static interface IntersectionCalculator {
        int[] intersect (ArrayAPT[] a, ArrayAPT[] b);
    }

    private final IntersectionCalculator intersector;
    private final Direction direction;

    public BinaryIntersectionComposer(IntersectionCalculator intersector, Direction direction) {
        this.intersector = intersector;
        this.direction = direction;
    }

    public BinaryIntersectionComposer(IntersectionCalculator intersector) {
        this(intersector, Direction.BOTTOM_UP);
    }

    @Override
    public ArrayAPT[] compose(DistributionalLexicon<?, ?, APT> lexicon, RGraph graph) {
        return new ArrayAPT[0];
    }

    private static int[] intersect(int[] a, int[] b) {
        int i = 0;
        int j = 0;
        int k = 0;
        int[] intersection = new int[Math.max(a.length, b.length)];

        while (i < a.length && j < b.length) {
            final int bj = b[j];
            while (i < a.length && a[i] < bj) {
                i += 2;
            }
            if (i < a.length) {
                final int ai = a[i];
                while (j < b.length && b[j] < ai) {
                    j += 2;
                }

                if (j < b.length) {
                    intersection[k++] = ai;
                    intersection[k++] = Math.min(a[i+1], b[j+1]);

                    i += 2;
                    j += 2;
                }
            }
        }

        if (intersection.length != k) {
            return Arrays.copyOf(intersection, k);
        } else {
            return intersection;
        }
    }

    private static float[] multiply(float[] arr, float alpha) {
        float[] result = arr.clone();
        for (int i=0; i<result.length;i++) result[i] *= alpha;
        return result;
    }

    public static final IntersectionCalculator JEREMY_CALCULATOR = new IntersectionCalculator() {
        @Override
        public int[] intersect(ArrayAPT[] a, ArrayAPT[] b) {
            double lo = 0;
            double hi = 1;

            final int resolution = 8;
            final double improvement_threshold = 0.01;

            double best_score = Double.MIN_VALUE;
            double current_score = 0;
            int[] best_intersection = null;

            while (current_score - best_score > improvement_threshold) {
                double alpha = lo;
                double best_alpha = lo;


            }

            return best_intersection;
        }
    };
}
