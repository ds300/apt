package uk.ac.susx.tag.apt.tasks;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LevelDBByteStore;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.Daemon;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public static void writeVector(ArrayAPT apt, Writer out, Resolver<String> entityIndexer, RelationIndexer relationIndexer, boolean normalise, boolean resolve, float sum) {
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

            if(resolve)
                writeVector(apt, out, entityIndexer, relationIndexer, normalise, true, sum.f);
            else
                writeVector(apt, out, normalise, sum.f);
            out.write("\n");

            numAptsProcessed.incrementAndGet();
        }


        watcher.stop();

        watcher.task.run();
    }

    public static void main(String[] args) throws IOException {
        Options opts = new Options();

        new JCommander(opts, args);


        String lexiconDirectory = opts.parameters.get(0);
        String outputFilename = opts.parameters.get(1);
        boolean normalise = opts.normalise;
        boolean resolve = !opts.compact;


        LexiconDescriptor lexiconDescriptor = LexiconDescriptor.from(lexiconDirectory);

        try (LRUCachedAPTStore<ArrayAPT> cachedAPTStore = new LRUCachedAPTStore.Builder()
                .setMaxDepth(Integer.MAX_VALUE)
                .setFactory(ArrayAPT.factory)
                .setBackend(LevelDBByteStore.fromDescriptor(lexiconDescriptor))
                .setMaxItems(opts.cacheSize)
                .build();
             Writer out = IO.writer(outputFilename)) {
            vectors(cachedAPTStore, lexiconDescriptor.getEntityIndexer(), lexiconDescriptor.getRelationIndexer(), out, resolve, normalise);
        }
    }

    public static class Options {
        @Parameter
        List<String> parameters = new ArrayList<>();

        @Parameter(names = {"cache-size"}, description = "The maximum size of the in-memory APT cache")
        public int cacheSize = 100000;
        @Parameter(names = {"-normalise"}, description = "Create Vectors with normalised counts")
        public boolean normalise = false;
        @Parameter(names = {"-compact"}, description = "Compact path indices into lemmas")
        public boolean compact = false;
    }
}
