package uk.ac.susx.tag.apt;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Created by ds300 on 15/07/2015.
 */
public class OverlayComposer implements APTComposer<ArrayAPT> {

    public final Function<ArrayList<OverlayAPT.ArrayAPTPathTuple>, ArrayAPT> collapser;

    public OverlayComposer(Function<ArrayList<OverlayAPT.ArrayAPTPathTuple>, ArrayAPT> collapser) {
        this.collapser = collapser;
    }

    public OverlayComposer() {
        // do simple sum
        this(new PairwiseCollapser()::collapse);
    }

    public static class PairwiseCollapser  {
        public final BinaryOperator<ArrayAPT> combine;

        public PairwiseCollapser(BinaryOperator<ArrayAPT> combine) {
            this.combine = combine;
        }

        public PairwiseCollapser() {
            this.combine = (a, b) -> a.merged(b.culled(0), 0);
        }

        public ArrayAPT collapse(ArrayList<OverlayAPT.ArrayAPTPathTuple> nodes) {
            return nodes.stream().map(x -> x.apt).reduce(ArrayAPT.factory.empty(), combine);
        }

    }

    public static class FloatFourTuple {
        float entityScore;
        float nodeScore;

        float pathEntityScore;
        float pathScore;

        public float toPMI() {
            return (float) Math.log( (entityScore/nodeScore) / (pathEntityScore/pathScore) );
        }
    }

    public static class SumStarCollpaser {
        public final PairwiseCollapser pc = new PairwiseCollapser();
        public final ArrayAPT everythingCounter;
        public final boolean ppmi;

        public SumStarCollpaser(ArrayAPT everythingCounter, boolean ppmi) {
            this.everythingCounter = everythingCounter;
            this.ppmi = ppmi;
        }

        public ArrayAPT collapse(ArrayList<OverlayAPT.ArrayAPTPathTuple> nodes) {
            ArrayAPT result = pc.collapse(nodes);
            FloatFourTuple[] tuples = new FloatFourTuple[result.entities.length];
            for (int i = 0; i < tuples.length; i++) {
                tuples[i] = new FloatFourTuple();
                tuples[i].nodeScore = result.sum();
            }

            for (OverlayAPT.ArrayAPTPathTuple tup : nodes) {
                ArrayAPT apt = tup.apt;
                int[] path = tup.path;
                ArrayAPT countNode = everythingCounter.getChildAt(path);
                if (countNode == null) {
                    System.err.println("countnode is null. This is whack");
                }

                float pathScore = countNode == null ? 1 : countNode.sum();

                for (int i=0; i<result.entities.length; i++) {
                    int eid = result.entities[i];
                    float score = apt.getScore(eid);
                    float pathEntityScore = countNode == null ? 0 : countNode.getScore(eid);


                    FloatFourTuple fourTuple = tuples[i];

                    fourTuple.entityScore += score;
                    fourTuple.pathScore += pathScore;
                    fourTuple.pathEntityScore += pathEntityScore;
                }
            }

            int posCount = 0;
            for (int i=0; i<result.entities.length; i++) {
                result.scores[i] = tuples[i].toPMI();
                if (ppmi && result.scores[i] > 0) {
                    posCount ++;
                }
            }

            if (ppmi) {
                int[] newEntities = new int[posCount];
                float[] newScores = new float[posCount];
                int x = 0;
                for (int i=0; i<result.entities.length; i++) {
                    if (result.scores[i] > 0) {
                        newEntities[x] = result.entities[i];
                        newScores[x] = result.scores[i];
                        x++;
                    }
                }
                result.entities = newEntities;
                result.scores = newScores;
            }

            if (result.edges.length > 0) {
                // this shouldn't happen
                System.err.println("Bad news dave");
                result.edges = ArrayAPT.EMPTY_INTS;
                result.kids = ArrayAPT.EMPTY_KIDS;
            }

            return result;
        }
    }



    @Override
    public ArrayAPT[] compose(PersistentKVStore<Integer, APT> lexicon, RGraph graph) throws IOException {
        final ArrayAPT[] collapsed = ArrayAPT.factory.fromGraph(graph);
        final int[][] pathsFromRoot = new int[collapsed.length][];
        int rootIdx = graph.sorted()[0];
        collapsed[rootIdx].walk((path, apt) ->  {
            for (int i = 0; i < collapsed.length; i++) {
                if (collapsed[i] == apt) {
                    pathsFromRoot[i] = path;
                }
            }
        });

        OverlayAPT root = new OverlayAPT();

        for (int i=0; i<collapsed.length; i++) {
            if (collapsed[i] != null) {
                // entity participates in dependency tree
                int eid = graph.entityIds[i];

                ArrayAPT fromLexicon = ArrayAPT.ensureArrayAPT(lexicon.get(eid));

                ArrayAPT offset = fromLexicon.getChildAt();

                if (offset == null) {
                    offset = fromLexicon.withEdge(pathsFromRoot[i], ArrayAPT.factory.empty()).getChildAt(pathsFromRoot[i]);
                }

                root.getOrMakeIn(pathsFromRoot[i]).overlay(offset);
            }
        }

        // ok all data is in
        // now build result apt.

        ArrayAPT collapsedRoot = root.collapse(collapser);

        for (int i=0; i<collapsed.length; i++) {
            if (collapsed[i] != null) {
                collapsed[i] = collapsedRoot.getChildAt(pathsFromRoot[i]);
            }
        }

        return collapsed;
    }

    public static OverlayComposer sumStar(ArrayAPT everythingCounts, boolean ppmi) {
        final SumStarCollpaser blah = new SumStarCollpaser(everythingCounts, ppmi);
        return new OverlayComposer(new Function<ArrayList<OverlayAPT.ArrayAPTPathTuple>, ArrayAPT>() {
            @Override
            public ArrayAPT apply(ArrayList<OverlayAPT.ArrayAPTPathTuple> arrayAPTPathTuples) {
                return blah.collapse(arrayAPTPathTuples);
            }
        });
    }
}
