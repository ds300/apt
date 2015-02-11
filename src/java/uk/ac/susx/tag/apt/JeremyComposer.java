package uk.ac.susx.tag.apt;




/**
 * Created by ds300 on 01/12/2014.
 */
public class JeremyComposer extends EdgeResolutionComposer {
    private JeremyComposer(EdgeResolver resolver, Direction direction) {
        super(resolver, direction);
    }

    public static class Builder {
        Direction direction = Direction.TOP_DOWN;
        float alpha_from = 0.5f;
        float alpha_to = 0.5f;
        int alpha_intervals = 1;


        public Builder bottomUp() {
            this.direction = Direction.BOTTOM_UP;
            return this;
        }

        public Builder topDown() {
            this.direction = Direction.TOP_DOWN;
            return this;
        }

        public Builder alpha(float alpha) {
            return alpha(alpha, alpha, 1);
        }

        public Builder alpha(float from, float to, int intervals) {
            alpha_from = from;
            alpha_to = to;
            alpha_intervals = intervals;
            return this;
        }

        public JeremyComposer build() {

            return new JeremyComposer(new NodeAlignmentEdgeResolver(new NodeIntersectionCalculator() {
                @Override
                public Int2FloatArraySortedMap intersect(Int2FloatArraySortedMap a, Int2FloatArraySortedMap b) {
                    final int[] keys_a = a.keys;
                    final float[] vals_a = a.vals;
                    final int[] keys_b = b.keys;
                    final float[] vals_b = b.vals;

                    if (keys_a.length == 0 && keys_b.length == 0) return a;

                    // keep track of best so far
                    float best_score = Float.MIN_VALUE;

                    int[] best_keys = null;
                    float[] best_vals = null;
                    float best_alpha = 0;


                    for (int interval = 0; interval < alpha_intervals; interval ++) {
                        float alpha_a = alpha_from + (((alpha_to - alpha_from) / alpha_intervals) * interval);
                        float alpha_b = 1 - alpha_a;

                        final int size = Util.countUnique(keys_a, keys_b);
                        int[] keys = new int[size];
                        float[] vals = new float[size];

                        int i=0, j=0, k=0;

                        while (i < keys_a.length && j < keys_b.length) {
                            int keya = keys_a[i];
                            int keyb = keys_b[j];
                            if (keya == keyb) {
                                keys[k] = keya;
                                vals[k] = (Math.max(vals_a[i], vals_b[j]) * alpha_a) + (Math.min(vals_a[i], vals_b[j]) * alpha_b);
                                i++;
                                j++;
                            } else if (keya < keyb) {
                                keys[k] = keya;
                                vals[k] = vals_a[i] * alpha_a;
                                i++;
                            } else {
                                keys[k] = keyb;
                                vals[k] = vals_b[j] * alpha_b;
                                j++;
                            }
                            k++;
                        }

                        while (i < keys_a.length) {
                            keys[k] = keys_a[i];
                            vals[k] = vals_a[i] * alpha_a;
                            i++;
                            k++;
                        }
                        while (j < keys_b.length) {
                            keys[k] = keys_b[j];
                            vals[k] = vals_b[j] * alpha_b;
                            j++;
                            k++;
                        }

                        float score = cosine(keys_a, vals_a, keys, vals) + cosine(keys_b, vals_b, keys, vals);
                        if (score > best_score) {
                            best_score = score;
                            best_alpha = alpha_a;
                            best_keys = keys;
                            best_vals = vals;
                        }
                    }

//                    System.out.println("best alpha: " + best_alpha);

                    return new Int2FloatArraySortedMap(best_keys, best_vals);
                }
            }), direction);
        }
    }


    private static float cosine(int[] keys_a, float[] vals_a, int[] keys_b, float[] vals_b) {
        float sum = 0;
        float suma = 0;
        float sumb = 0;
        int i=0, j=0;

        while (i < keys_a.length && j < keys_b.length) {
            int keya = keys_a[i];
            int keyb = keys_b[j];
            float vala = vals_a[i];
            float valb = vals_b[j];
            if (keya == keyb) {
                sum += vala * valb;
                suma += Math.pow(vala, 2);
                i++;
                sumb += Math.pow(valb, 2);
                j++;
            } else if (keya < keyb) {
                suma += Math.pow(vala, 2);
                i++;
            } else {
                sumb += Math.pow(valb, 2);
                j++;
            }
        }

        while (i < keys_a.length) {
            suma += Math.pow(vals_a[i], 2);
            i++;
        }

        while (j < keys_b.length) {
            sumb += Math.pow(vals_b[j], 2);
            j++;
        }

        return (float)(sum / (Math.sqrt(suma) + Math.sqrt(sumb)));
    }
}
