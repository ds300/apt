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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by ds300 on 21/09/2015.
 */
public class Compose {

    static final AtomicInteger failedSents = new AtomicInteger(0);
    static final AtomicInteger partiallyFailedSents = new AtomicInteger(0);
    static final AtomicInteger sentId = new AtomicInteger(0);

    static final LinkedBlockingQueue<List<String[]>> sentenceQ = new LinkedBlockingQueue<>();
    static ExecutorService readerPool = Executors.newFixedThreadPool(2);
    static ExecutorService composerPool;
    static Daemon watcher;


    public static class Options {
        @Parameter
        public List<String> parameters = new ArrayList<>();

        @Parameter(names = {"cache-size"}, description = "The maximum size of the in-memory APT cache")
        public int cacheSize = 100000;

        @Parameter(names = {"-threads"}, description = "The maximum size of the in-memory APT cache")
        public int threads = 2;

        @Parameter(names = {"method"}, description = "The method of composition to use. One of: sum, sum*")
        public String method = "sum*";

        @Parameter(names = {"pmi"}, description = "Use standard pmi rather than ppmi. (only applies to the sum* method)")
        public boolean pmi = false;

        @Parameter(names = {"-skip-trees"}, description = "If true, serialised composed APTs will not be written to disk, just vector representation")
        public boolean skipTrees = false;

        @Parameter(names = {"-vectors"}, description = "Also output a vector representation of the composed APT")
        public boolean vectors = false;

        @Parameter(names = {"-compact"}, description = "Compact path indices into lemmas")
        public boolean compact = false;

        @Override
        public String toString() {
            return "Options{" +
                    "parameters=" + parameters +
                    ", cacheSize=" + cacheSize +
                    ", threads=" + threads +
                    ", method='" + method + '\'' +
                    ", pmi=" + pmi +
                    ", skipTrees=" + skipTrees +
                    ", vectors=" + vectors +
                    ", compact=" + compact +
                    '}';
        }
    }

    private static String pad(int i) {
        String s = Integer.toString(i);
        while (s.length() < 12) {
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
            watcher = new Daemon(() -> {
                String time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
                System.out.printf("%s -- %d sentences composed\n", time, sentId.get());
            }, 5000);

            watcher.start();
            composerPool = Executors.newFixedThreadPool(opts.threads);
            for (File file : files) {
                readerPool.submit(new SentenceProducer(file));
                File outputDir = new File(file.getParent(), file.getName() + "-composed");
                if (!outputDir.exists() && !outputDir.mkdirs()) {
                    throw new RuntimeException("can't create directory " + outputDir.getAbsolutePath());
                }
                for (int i = 0; i < 5; i++) {
                    composerPool.submit(new SentenceConsumer(composer, entityIndexer, relationIndexer, lexiconStore, opts, outputDir));
                }
                readerPool.shutdown();
                composerPool.shutdown();
                composerPool.awaitTermination(25, TimeUnit.HOURS);
                watcher.stop();
                watcher.task.run();
                System.out.println("Partially failed compositions: " + partiallyFailedSents);
                System.out.println("Failed compositions: " + failedSents);
            }
        }
    }

    static class SentenceProducer implements Runnable {
        private final File sentenceFile;

        public SentenceProducer(File sentenceFile) {
            this.sentenceFile = sentenceFile;
        }

        @Override
        public void run() {
            try (ConllReader<String[]> sentencesFromFile = ConllReader.from(IO.reader(sentenceFile))) {
                for (List<String[]> sentence : sentencesFromFile) {
                    sentenceQ.put(sentence);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    static class SentenceConsumer implements Runnable {
        private APTComposer<ArrayAPT> composer;
        private Indexer<String> entityIndexer;
        private RelationIndexer relationIndexer;
        private LRUCachedAPTStore lexiconStore;
        private Options opts;
        private File outputDir;

        public SentenceConsumer(APTComposer<ArrayAPT> composer, Indexer<String> entityIndexer,
                                RelationIndexer relationIndexer, LRUCachedAPTStore lexiconStore,
                                Options opts, File outputDir) {
            this.composer = composer;
            this.entityIndexer = entityIndexer;
            this.relationIndexer = relationIndexer;
            this.lexiconStore = lexiconStore;
            this.opts = opts;
            this.outputDir = outputDir;
        }

        @Override
        public void run() {
            List<String[]> sentence = null;
            while (true) {
                try {
                    // wait for new sentences for a few seconds, terminate if no new data available
                    sentence = sentenceQ.poll(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (sentence != null) {
                    try {
                        doComposition(composer, entityIndexer, relationIndexer, lexiconStore, opts, outputDir, sentence);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else return;
            }
        }
    }

    public static void doComposition(APTComposer<ArrayAPT> composer, Indexer<String> entityIndexer, RelationIndexer relationIndexer,
                                     LRUCachedAPTStore lexiconStore, Options opts, File outputDir,
                                     List<String[]> sentence) throws IOException {
        RGraph graph = Construct.sentence2Graph(entityIndexer, relationIndexer, sentence);
        ArrayAPT[] composed = composer.compose(lexiconStore, graph);
        ArrayAPT rootNode = composed[graph.sorted()[0]];
        if (rootNode.getEntityCount() == 0)
            failedSents.getAndIncrement();
        else if (rootNode.getEntityCount() < sentence.size())
            partiallyFailedSents.getAndIncrement();
        int[][] paths = new int[composed.length][];
        rootNode.walk((path, apt) -> {
            for (int i = 0; i < composed.length; i++) {
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

        if (!opts.skipTrees) {
            try (OutputStream out = IO.outputStream(new File(outputDir, pad(sentId.get()) + ".apt.gz"))) {
                out.write(rootNode.toByteArray());
            }
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

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        new JCommander(opts, args);
        System.out.println(opts);
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
