package uk.ac.susx.tag.apt.tasks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LevelDBByteStore;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.ConllReader;
import uk.ac.susx.tag.apt.util.Daemon;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by ds300 on 21/09/2015.
 */
public class Compose {

    public static class Options {
        @Parameter
        public List<String> parameters = new ArrayList<>();

        @Parameter(names = {"cache-size"}, description = "The maximum size of the in-memory APT cache")
        public int cacheSize = 100000;

        @Parameter(names = {"method"}, description = "The method of composition to use. One of: sum, sum*")
        public String method = "sum*";

        @Parameter(names = {"pmi"}, description = "Use standard pmi rather than ppmi. (only applies to the sum* method)")
        public boolean pmi = false;

        @Parameter(names = {"-vectors"}, description = "Also output a vector representation of the composed APT")
        public boolean vectors = false;

        @Parameter(names = {"-compact"}, description = "Compact path indices into lemmas")
        public boolean compact = false;
    }

    private static String pad(int i) {
        String s = Integer.toString(i);
        while (s.length() < 8) {
            s = "0" + s;
        }
        return s;
    }

    private static String readablePath(int[] path, RelationIndexer relationIndexer) {
        StringBuilder sb = new StringBuilder();
        if (path.length > 0) {
            sb.append(relationIndexer.resolve(path[0]));
            for (int i = 1; i < path.length; i++) {
                sb.append("»");
                sb.append(relationIndexer.resolve(path[i]));
            }
        }
        return sb.toString();
    }

    private static String readablePath(int[] path) {
        StringBuilder sb = new StringBuilder();
        if (path.length > 0) {
            sb.append(path[0]);
            for (int i = 1; i < path.length; i++) {
                sb.append("»");
                sb.append(path[i]);
            }
        }
        return sb.toString();
    }

    public static void compose(LexiconDescriptor descriptor,
                               APTComposer<ArrayAPT> composer,
                               Collection<File> files,
                               Options opts) throws Exception {

        final Indexer<String> entityIndexer = descriptor.getEntityIndexer();
        final RelationIndexer relationIndexer = descriptor.getRelationIndexer();

        try (final LRUCachedAPTStore lexiconStore = new LRUCachedAPTStore.Builder<ArrayAPT>()
                .setMaxDepth(Integer.MAX_VALUE)
                .setFactory(ArrayAPT.factory)
                .setBackend(LevelDBByteStore.fromDescriptor(descriptor))
                .setMaxItems(opts.cacheSize)
                .build()) {
            for (File file : files) {
                File outputDir = new File(file.getParent(), file.getName() + "-composed");
                if (!outputDir.exists() && !outputDir.mkdirs()) {
                    throw new RuntimeException("can't create directory " + outputDir.getAbsolutePath());
                }

                final AtomicInteger sentId = new AtomicInteger(0);
                int failedSents = 0, partiallyFailedSents = 0;
                try (ConllReader<String[]> sents = ConllReader.from(IO.reader(file))) {
                    final Daemon watcher = new Daemon(() -> {
                        System.out.println(sentId.get() + " sentences composed");
                    }, 5000);

                    watcher.start();

                    for (List<String[]> sentence : sents) {
                        RGraph graph = Construct.sentence2Graph(entityIndexer, relationIndexer, sentence);
                        ArrayAPT[] composed = composer.compose(lexiconStore, graph);
                        ArrayAPT rootNode = composed[graph.sorted()[0]];
                        if (rootNode.getEntityCount() == 0)
                            failedSents++;
                        else if (rootNode.getEntityCount() < sentence.size())
                            partiallyFailedSents++;
                        int[][] paths = new int[composed.length][];
                        rootNode.walk((path, apt) -> {
                            for (int i=0; i < composed.length; i++) {
                                if (composed[i] == apt) {
                                    paths[i] = path;
                                }
                            }
                        });

                        try (Writer out = IO.writer(new File(outputDir, pad(sentId.get()) + ".sent"))) {
                            for (String[] token : sentence) {
                                if (token.length == 4) {
                                    int id = Integer.parseInt(token[0]) - 1;
                                    String[] newToken = new String[6];
                                    System.arraycopy(token, 0, newToken, 0, 4);
                                    newToken[4] = readablePath(paths[id], relationIndexer);
                                    newToken[5] = readablePath(paths[id]);

                                    token = newToken;
                                }
                                if (token.length >= 1) {
                                    out.write(token[0]);
                                }
                                for (int i = 1; i < token.length; i++) {
                                    out.write("\t");
                                    out.write(token[i]);
                                }
                                out.write("\n");
                            }
                            out.write("\n");
                        }

                        try (OutputStream out = IO.outputStream(new File(outputDir, pad(sentId.get()) + ".apt.gz"))) {
                            out.write(rootNode.toByteArray());
                        }

                        if (opts.vectors) {
                            File outputFile = new File(outputDir, pad(sentId.get()) + ".apt.vec.gz");
                            try (Writer out = IO.writer(outputFile)) {
                                if (opts.compact)
                                    Vectors.writeVector(rootNode, out, false, rootNode.sum());
                                else
                                    Vectors.writeVector(rootNode, out, (Resolver<String>) entityIndexer, relationIndexer, false, true, rootNode.sum());
                            }
                        }

                        sentId.incrementAndGet();
                    }
                    watcher.task.run();
                    watcher.stop();
                    System.out.println("Partially failed compositions: " + partiallyFailedSents);
                    System.out.println("Failed compositions: " + failedSents);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        new JCommander(opts, args);

        String dir = opts.parameters.get(0);
        LexiconDescriptor descriptor = LexiconDescriptor.from(dir);

        Collection<File> files = opts.parameters
                .subList(1, opts.parameters.size())
                .stream()
                .map(File::new)
                .collect(Collectors.toList());

        APTComposer composer;

        switch (opts.method) {
            case "sum":
                composer = new OverlayComposer();
                break;
            case "sum*":
                composer = OverlayComposer.sumStar(descriptor.getEverythingCounts(), !opts.pmi);
                break;
            default:
                throw new IllegalArgumentException("'" + opts.method + "' is not a composition method");
        }

        compose(descriptor, composer, files, opts);

    }
}
