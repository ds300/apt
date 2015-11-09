package uk.ac.susx.tag.apt.tasks;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LevelDBByteStore;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.Daemon;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.ParameterValidator;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ds300 on 21/09/2015.
 */
public class Vectors {

    /**
     * Write vectors without resolving the paths, e.g. the three-feature vector
     * see/V	:i/LS	5.0	:see/V	190.0	:in/CONJ	5.0
     *
     * becomes
     *
     * see/V	:0	5.0	:2	190.0	:10	5.0
     *
     */
    public static void writeVector(ArrayAPT apt, Writer out, boolean normalise, float sum) {
        writeVector(apt, out, null, null, normalise, false, sum);
    }

    // TODO: could do with a more elegant solution to ppmi vs. non-ppmi vectors
    public static void writePPMIVector(ArrayAPT apt, Writer out, boolean normalise, float sum,
                                       Map<String, Float> pathMap, Map<String, Float> eventMap) {
        writePPMIVector(apt, out, null, null, normalise, false, sum, pathMap, eventMap);
    }

    // TODO: Support normalised PPMI vectors
    public static void writePPMIVector(ArrayAPT apt, Writer out, Resolver<String> entityIndexer,
                                       RelationIndexer relationIndexer, boolean normalise, boolean resolve, float sum,
                                       Map<String, Float> pathMap, Map<String, Float> eventMap) {
        // Actually walk the shit and do stuff
        apt.walk((path, node) -> {
            StringBuilder pathStringBuilder = new StringBuilder();

            if (path.length > 0) {
                pathStringBuilder.append(resolve ? relationIndexer.resolve(path[0]) : Integer.toString(path[0]));
            }

            for (int i = 1; i < path.length; i++) {
                pathStringBuilder.append("»");
                pathStringBuilder.append(resolve ? relationIndexer.resolve(path[i]) : Integer.toString(path[i]));
            }

            pathStringBuilder.append(":");

            final String pathString = pathStringBuilder.toString();

            String p = pathString.equals(":") ? "__EPSILON__" : pathString.substring(0, pathString.length() - 2);
            final float pathScore = pathMap.get(p);

            System.out.println(p);
            System.out.println(pathMap);

            ((ArrayAPT) node).forEach((eid, score) -> {
                try {
                    out.write(pathString);
                    out.write(resolve ? entityIndexer.resolve(eid) : eid.toString());
                    out.write("\t");
                    out.write(Double.toString(Math.log(((score * pathScore) / (node.sum() * eventMap.get(pathString + eid.toString())))) < 0.d ? 0.d : Math.log(((score * pathScore) / (node.sum() * eventMap.get(pathString + eid.toString()))))));
                    out.write("\t");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        });

    }

    public static void writeVector(ArrayAPT apt, Writer out, Resolver<String> entityIndexer,
                                   RelationIndexer relationIndexer, boolean normalise, boolean resolve, float sum) {
        // Actually walk the shit and do stuff
        apt.walk((path, node) -> {
            StringBuilder pathStringBuilder = new StringBuilder();

            if (path.length > 0) {
                pathStringBuilder.append(resolve ? relationIndexer.resolve(path[0]) : Integer.toString(path[0]));
            }

            for (int i = 1; i < path.length; i++) {
                pathStringBuilder.append("»");
                pathStringBuilder.append(resolve ? relationIndexer.resolve(path[i]) : Integer.toString(path[i]));
            }

            pathStringBuilder.append(":");

            final String pathString = pathStringBuilder.toString();

            ((ArrayAPT) node).forEach((eid, score) -> {
                try {
                    out.write(pathString);
                    out.write(resolve ? entityIndexer.resolve(eid) : eid.toString());
                    out.write("\t");
                    out.write(Float.toString(normalise ? score / sum : score));
                    out.write("\t");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }

    public static void vectors (final LRUCachedAPTStore<ArrayAPT> aptStore,
                                final Resolver<String> entityIndexer,
                                final RelationIndexer relationIndexer,
                                final Writer out,
                                final boolean resolve,
                                final boolean normalise) throws IOException {


        final AtomicInteger numAptsProcessed = new AtomicInteger(0);

        final Daemon watcher = new Daemon(() -> {
            System.out.println(numAptsProcessed.get() + " apts processed");
        }, 5000);

        watcher.start();

        for (Map.Entry<Integer, ArrayAPT> entry : aptStore) {
            int entityId = entry.getKey();
            ArrayAPT apt = entry.getValue();

            out.write(entityIndexer.resolve(entityId));
            out.write("\t");

            class mutableFloat {
                float f = 0;
            }
			final mutableFloat sum = new mutableFloat();
            apt.walk((path, a) -> {
                sum.f += a.sum();
            });
			// For the normalisation business it might be easiest to walk every path twice

            if (resolve)
                writeVector(apt, out, entityIndexer, relationIndexer, normalise, true, sum.f);
            else
                writeVector(apt, out, normalise, sum.f);

            out.write("\n");

            numAptsProcessed.incrementAndGet();
        }


        watcher.stop();

        watcher.task.run();
    }

    public static void ppmiVectors(final LRUCachedAPTStore<ArrayAPT> aptStore,
                                   final Resolver<String> entityIndexer,
                                   final RelationIndexer relationIndexer,
                                   final Writer out,
                                   final boolean resolve,
                                   final boolean normalise) throws IOException {
        final AtomicInteger numAptsProcessed = new AtomicInteger(0);

        final Daemon watcher = new Daemon(() -> {
            System.out.println(numAptsProcessed.get() + " apts processed");
        }, 5000);

        watcher.start();

        Map<String, Float> pathMap = new HashMap<>();
        Map<String, Float> eventMap = new HashMap<>();

        // Collect necessary counts
        for (Map.Entry<Integer, ArrayAPT> entry : aptStore) {
            int entityId = entry.getKey();
            ArrayAPT apt = entry.getValue();

            apt.walk((path, node) -> {
                StringBuilder pathStringBuilder = new StringBuilder();

                if (path.length > 0) {
                    pathStringBuilder.append(Integer.toString(path[0]));
                }

                for (int i = 1; i < path.length; i++) {
                    pathStringBuilder.append("»");
                    pathStringBuilder.append(Integer.toString(path[i]));
                }

                String p = pathStringBuilder.toString().equals("") ? "__EPSILON__" : pathStringBuilder.toString();

                pathMap.compute(p, (k, v) -> v == null ? 1.f : v + 1.f);

                ((ArrayAPT) node).forEach((eid, score) -> {
                    eventMap.compute(String.format("%s:%d", pathStringBuilder.toString(), entityId),
                            (k, v) -> v == null ? 1.f : v + 1.f);
                });
            });
        }

        // Run stuff again to produce vectors
        for (Map.Entry<Integer, ArrayAPT> entry : aptStore) {
            int entityId = entry.getKey();
            ArrayAPT apt = entry.getValue();

            out.write(entityIndexer.resolve(entityId));
            out.write("\t");

            if (resolve)
                writePPMIVector(apt, out, entityIndexer, relationIndexer, normalise, true, 0.f, pathMap, eventMap);
            else
                writePPMIVector(apt, out, normalise, 0.f, pathMap, eventMap);

            out.write("\n");

            numAptsProcessed.incrementAndGet();

        }
    }

    public static void main(String[] args) throws IOException {
        Options opts = new Options();

        new JCommander(opts, args);
        System.out.println(opts);

        ParameterValidator.atLeast(opts.parameters, 2);
        String lexiconDirectory = opts.parameters.get(0);
        ParameterValidator.isLexicon(lexiconDirectory);
        String outputFilename = opts.parameters.get(1);
        boolean normalise = opts.normalise;
        boolean resolve = !opts.compact;
        boolean ppmi = opts.ppmi;


        LexiconDescriptor lexiconDescriptor = LexiconDescriptor.from(lexiconDirectory);

        try (LRUCachedAPTStore<ArrayAPT> cachedAPTStore = new LRUCachedAPTStore.Builder()
                .setMaxDepth(Integer.MAX_VALUE)
                .setFactory(ArrayAPT.factory)
                .setBackend(LevelDBByteStore.fromDescriptor(lexiconDescriptor))
                .setMaxItems(opts.cacheSize)
                .build();

             Writer out = IO.writer(outputFilename)) {

                if (!ppmi) {
                    vectors(cachedAPTStore, lexiconDescriptor.getEntityIndexer(), lexiconDescriptor.getRelationIndexer(), out, resolve, normalise);
                } else {
                    ppmiVectors(cachedAPTStore, lexiconDescriptor.getEntityIndexer(), lexiconDescriptor.getRelationIndexer(), out, resolve, normalise);
                }
        }
    }

    public static class Options {
        @Parameter
        List<String> parameters = new ArrayList<>();

        @Parameter(names = {"cache-size"}, description = "The maximum size of the in-memory APT cache")
        public int cacheSize = 100000;

        @Parameter(names = {"-normalise"}, description = "Create Vectors with normalised counts")
        public boolean normalise = false;

        @Parameter(names = {"-ppmi"}, description = "Create ppmi weighted Vectors")
        public boolean ppmi = false;

        @Parameter(names = {"-compact"}, description = "Compact path indices into lemmas")
        public boolean compact = false;

        @Override
        public String toString() {
            return "Options{" +
                    "parameters=" + parameters +
                    ", cacheSize=" + cacheSize +
                    ", normalise=" + normalise +
                    ", ppmi=" + ppmi +
                    ", compact=" + compact +
                    '}';
        }
    }
}
