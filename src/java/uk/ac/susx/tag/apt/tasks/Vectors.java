package uk.ac.susx.tag.apt.tasks;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import javassist.compiler.Lex;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LevelDBByteStore;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.Daemon;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ds300 on 21/09/2015.
 */
public class Vectors {



    public static void vectors (final LRUCachedAPTStore<ArrayAPT> aptStore,
                                final Resolver<String> entityIndexer,
                                final RelationIndexer relationIndexer,
                                final Writer out) throws IOException {


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

            apt.walk((path, node) -> {
                StringBuilder pathStringBuilder = new StringBuilder();

                if (path.length > 0) {
                    pathStringBuilder.append(relationIndexer.resolve(path[0]));
                }

                for (int i = 1; i < path.length; i++) {
                    pathStringBuilder.append("»");
                    pathStringBuilder.append(relationIndexer.resolve(path[i]));
                }

                pathStringBuilder.append(":");

                final String pathString = pathStringBuilder.toString();

                ((ArrayAPT) node).forEach((eid, score) -> {
                    try {
                        out.write(pathString);
                        out.write(entityIndexer.resolve(eid));
                        out.write("\t");
                        out.write(Float.toString(score));
                        out.write("\t");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            });

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


        LexiconDescriptor lexiconDescriptor = LexiconDescriptor.from(lexiconDirectory);

        try (LRUCachedAPTStore<ArrayAPT> cachedAPTStore = new LRUCachedAPTStore.Builder()
                .setMaxDepth(Integer.MAX_VALUE)
                .setFactory(ArrayAPT.factory)
                .setBackend(LevelDBByteStore.fromDescriptor(lexiconDescriptor))
                .setMaxItems(opts.cacheSize)
                .build();
             Writer out = IO.writer(outputFilename)) {
            vectors(cachedAPTStore, lexiconDescriptor.getEntityIndexer(), lexiconDescriptor.getRelationIndexer(), out);
        }
    }

    public static class Options {
        @Parameter
        List<String> parameters = new ArrayList<>();

        @Parameter(names = {"cache-size"}, description = "The maximum size of the in-memory APT cache")
        public int cacheSize = 100000;
    }
}
