package uk.ac.susx.tag.apt.tasks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LevelDBByteStore;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ds300 on 17/09/2015.
 */
public class Construct {


    final LexiconDescriptor descriptor;
    final int depth;
    final Indexer<String> entityIndexer;
    final RelationIndexer relationIndexer;
    final AccumulativeAPTStore lexiconStore;
    final AccumulativeLazyAPT everythingCounter;


    final LinkedBlockingQueue<File> files = new LinkedBlockingQueue<>();
    final LinkedBlockingQueue<List<String[]>> sentences = new LinkedBlockingQueue<>(100);
    final Set<String> blacklist = new HashSet<>();

    final ExecutorService sentenceExtractors;
    final ExecutorService sentenceConsumers;

    final AtomicInteger numSentencesProcessed = new AtomicInteger(0);
    long lastReportTime = System.currentTimeMillis();
    long lastReportNumSents = 0;

    final Daemon reporter;

    private Construct(LexiconDescriptor descriptor,
                      int depth,
                      Indexer<String> entityIndexer,
                      RelationIndexer relationIndexer,
                      AccumulativeAPTStore lexiconStore,
                      AccumulativeLazyAPT everythingCounter,
                      Collection<File> files,
                      Set<String> blacklist,
                      ExecutorService sentenceExtractors,
                      ExecutorService sentenceConsumers) {
        this.descriptor = descriptor;
        this.depth = depth;
        this.entityIndexer = entityIndexer;
        this.relationIndexer = relationIndexer;
        this.lexiconStore = lexiconStore;
        this.everythingCounter = everythingCounter;
        this.files.addAll(files);
        this.blacklist.addAll(blacklist);
        this.sentenceExtractors = sentenceExtractors;
        this.sentenceConsumers = sentenceConsumers;

        this.reporter = new Daemon(() -> {
            MemoryReport mr = MemoryReport.get();
            long numSents = numSentencesProcessed.get();
            long currentTime = System.currentTimeMillis();
            long timeElapsed = currentTime - lastReportTime;
            long sentsElapsed = numSents - lastReportNumSents;
            String sentsPerSecond = timeElapsed == 0 ? "inf" : Long.toString(Math.round((double) sentsElapsed / ((double) timeElapsed / 1000)));
            String time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());

            if (lexiconStore.isClearingCache()) {
                System.out.printf(
                        "%s -- Awaiting Cache Clearance. %s items cleared. Using %s of %s.\n",
                        time, lexiconStore.numCacheItemsCleared(), mr.used.humanReadable(), mr.max.humanReadable()
                );
            } else {
                System.out.printf(
                        "%s -- %s Sentences processed. Using %s of %s. Going at %s sents/s\n",
                        time,
                        numSents,
                        mr.used.humanReadable(),
                        mr.max.humanReadable(),
                        sentsPerSecond);
            }
            System.out.flush();

            lastReportTime = currentTime;
            lastReportNumSents = numSents;
        }, 5000);
    }


    class SentenceExtractor implements Runnable {
        @Override
        public void run() {
            File sentenceFile;
            while ((sentenceFile = files.poll()) != null) {
                try (ConllReader<String[]> sentencesFromFile = ConllReader.from(IO.reader(sentenceFile))) {
                    for (List<String[]> sentence : sentencesFromFile) {
                        sentences.put(sentence);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class SentenceConsumer implements Runnable {
        @Override
        public void run() {
            while (true) {
                List<String[]> sentence = null;
                try {
                    sentence = sentences.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (sentenceExtractors.isTerminated()) {
                    return;
                } else if (sentence != null) {
                    try {
                        consumeGraph(sentence2Graph(sentence));
                        numSentencesProcessed.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    RGraph sentence2Graph(List<String[]> sentence) {
        return sentence2Graph(entityIndexer, relationIndexer, sentence);
    }

    public static RGraph sentence2Graph(Indexer entityIndexer, RelationIndexer relationIndexer, List<String[]> sentence) {
        // find max entity index
        int max = 0;
        for (String[] entity : sentence) {
            int i = Integer.parseInt(entity[0]);
            if (i > max) {
                max = i;
            }
        }

        final RGraph graph = new RGraph(max);
        final int[] entityIDs = graph.entityIds;

        for (String[] entity : sentence) {
            final int entityIdx = Integer.parseInt(entity[0]) - 1;
            final String entityValue = entity[1];

            entityIDs[entityIdx] = entityIndexer.getIndex(entityValue);

            if (entity.length == 4 && entity[2] != null && entity[2].length() > 0) {
                final int governorOffset = Integer.parseInt(entity[2]) - 1;
                final String relationType = entity[3];
                graph.addRelation(entityIdx, governorOffset, relationIndexer.getIndex(relationType));
            }
        }

        return graph;
    }

    void consumeGraph(RGraph graph) throws Exception {
        ArrayAPT[] apts = ArrayAPT.factory.fromGraph(graph);

        for (int i = 0; i < apts.length; i++) {
            final String entity = ((IndexerImpl) entityIndexer).resolve(graph.entityIds[i]);
            if (this.blacklist.contains(entity)) continue; // filter out blacklisted entities
            ArrayAPT apt = apts[i];
            if (apt != null) {
                lexiconStore.include(graph.entityIds[i], apt);
                everythingCounter.merge(apt, depth, 0);
            }
        }
    }

    void start(int numSentenceExtractors, int numSentenceConsumers) {
        reporter.start();
        for (int i = 0; i < numSentenceExtractors; i++) {
            sentenceExtractors.submit(new SentenceExtractor());
        }
        for (int i = 0; i < numSentenceConsumers; i++) {
            sentenceConsumers.submit(new SentenceConsumer());
        }
        sentenceExtractors.shutdown();
        sentenceConsumers.shutdown();
    }

    void awaitShutdown(long time, TimeUnit timeUnit) throws InterruptedException {
        sentenceConsumers.awaitTermination(time, timeUnit);
        reporter.stop();
        reporter.task.run(); // one last time yo
    }

    public static void construct(LexiconDescriptor lexiconDescriptor, int depth, Collection<File> files, Set<String> blacklist) throws IOException, InterruptedException {
        AccumulativeLazyAPT everythingCounter = new AccumulativeLazyAPT();
        IndexerImpl entityIndexer = lexiconDescriptor.getEntityIndexer();
        RelationIndexer relationIndexer = lexiconDescriptor.getRelationIndexer();

        int numSentenceExtractors = 5;
        int numSentenceConsumers = Runtime.getRuntime().availableProcessors() + 2;

        try (AccumulativeAPTStore store = new AccumulativeAPTStore(LevelDBByteStore.fromDescriptor(lexiconDescriptor), depth)) {
            System.out.println("Beginning Lexicon Construction");
            Construct construct = new Construct(
                    lexiconDescriptor,
                    depth,
                    entityIndexer,
                    relationIndexer,
                    store,
                    everythingCounter,
                    files,
                    blacklist,
                    Executors.newFixedThreadPool(numSentenceExtractors),
                    Executors.newFixedThreadPool(numSentenceConsumers));

            construct.start(numSentenceExtractors, numSentenceConsumers);
            construct.awaitShutdown(Long.MAX_VALUE, TimeUnit.DAYS);
            System.out.println("Finished Lexicon Construction");
        } finally {
            System.out.println("Storing everything counts");
            lexiconDescriptor.storeEverythingCounts(everythingCounter);
            System.out.println("Storing entity indexer");
            lexiconDescriptor.storeEntityIndexer(entityIndexer);
            System.out.println("Storing relation indexer");
            lexiconDescriptor.storeRelationIndexer(relationIndexer);
            System.out.println("totally done now");
        }

    }

    public static Stream<File> extractFilesRecursively(File f) {
        if (f.isDirectory()) {
            return Arrays.asList(f.listFiles()).stream().flatMap(Construct::extractFilesRecursively);
        } else {
            return Stream.of(f);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Options opts = new Options();
        new JCommander(opts, args);

        String dir = opts.parameters.get(0);
        if (opts.clean)
            FileUtils.deleteDirectory(new File(dir));

        Collection<File> files = opts.parameters
                .subList(1, opts.parameters.size())
                .stream()
                .map(File::new)
                .flatMap(Construct::extractFilesRecursively)
                .collect(Collectors.toList());

        HashSet<String> bl = opts.blacklist == null ? new HashSet<>() : new HashSet<>(Files.readAllLines(Paths.get(opts.blacklist)));
        construct(LexiconDescriptor.from(dir), opts.depth, files, bl);
    }

    public static class Options {
        @Parameter
        public List<String> parameters = new ArrayList<>();

        @Parameter(names = {"-depth"}, description = "Depth of trees to include")
        public Integer depth = 3;

        @Parameter(names = {"-blacklist"}, description = "List of entries that an elementary APT will not be built for")
        public String blacklist = null;

        @Parameter(names = {"-clean"}, description = "Delete output directory if it exists")
        public boolean clean = false;

    }
}
