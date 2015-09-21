package uk.ac.susx.tag.apt.tasks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LevelDBByteStore;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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


    final ConcurrentLinkedQueue<File> files = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<List<String[]>> sentences = new ConcurrentLinkedQueue<>();

    final ExecutorService sentenceExtractors;
    final ExecutorService sentenceConsumers;

    final AtomicInteger numSentencesProcessed = new AtomicInteger(0);
    final Daemon reporter = new Daemon(() -> {
        MemoryReport mr = MemoryReport.get();
        System.out.printf(
                "%s Sentences processed. Using %s of %s.\n",
                numSentencesProcessed.get(),
                mr.used.humanReadable(),
                mr.max.humanReadable());
    }, 5000);

    private Construct(LexiconDescriptor descriptor,
                      int depth,
                      Indexer<String> entityIndexer,
                      RelationIndexer relationIndexer,
                      AccumulativeAPTStore lexiconStore,
                      AccumulativeLazyAPT everythingCounter,
                      Collection<File> files,
                      ExecutorService sentenceExtractors,
                      ExecutorService sentenceConsumers) {
        this.descriptor = descriptor;
        this.depth = depth;
        this.entityIndexer = entityIndexer;
        this.relationIndexer = relationIndexer;
        this.lexiconStore = lexiconStore;
        this.everythingCounter = everythingCounter;
        this.files.addAll(files);
        this.sentenceExtractors = sentenceExtractors;
        this.sentenceConsumers = sentenceConsumers;
    }


    class SentenceExtractor implements Runnable {
        @Override
        public void run() {
            File sentenceFile;
            while ((sentenceFile = files.poll()) != null) {
                try (ConllReader<String[]> sentencesFromFile = ConllReader.from(IO.reader(sentenceFile))) {
                    for (List<String[]> sentence : sentencesFromFile) {
                        sentences.add(sentence);
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
            while (!sentenceExtractors.isShutdown() || !sentences.isEmpty()) {
                List<String[]> sentence = sentences.poll();
                while (!sentenceExtractors.isShutdown() && sentence == null) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ignored) { }

                    sentence = sentences.poll();
                }
                if (sentence == null) {
                    // extractors finished so so are we
                    return;
                } else {
                    // consume sentence
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
        final RGraph graph = new RGraph(sentence.size());
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

    public static void construct(LexiconDescriptor lexiconDescriptor, int depth, Collection<File> files) throws IOException, InterruptedException {
        AccumulativeLazyAPT everythingCounter = new AccumulativeLazyAPT();
        IndexerImpl entityIndexer = lexiconDescriptor.getEntityIndexer();
        RelationIndexer relationIndexer = lexiconDescriptor.getRelationIndexer();

        int numSentenceExtractors = 5;
        int numSentenceConsumers = Runtime.getRuntime().availableProcessors() + 2;

        try (AccumulativeAPTStore store = new AccumulativeAPTStore(LevelDBByteStore.fromDescriptor(lexiconDescriptor), depth)) {
            Construct construct = new Construct(
                    lexiconDescriptor,
                    depth,
                    entityIndexer,
                    relationIndexer,
                    store,
                    everythingCounter,
                    files,
                    Executors.newFixedThreadPool(numSentenceExtractors),
                    Executors.newFixedThreadPool(numSentenceConsumers));

            construct.start(numSentenceExtractors, numSentenceConsumers);
            construct.awaitShutdown(Long.MAX_VALUE, TimeUnit.DAYS);
        } finally {
            lexiconDescriptor.storeEverythingCounts(everythingCounter);
            lexiconDescriptor.storeEntityIndexer(entityIndexer);
            lexiconDescriptor.storeRelationIndexer(relationIndexer);
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Options opts = new Options();
        new JCommander(opts, args);

        construct(
                LexiconDescriptor.from(opts.directory),
                opts.depth,
                opts.parameters.stream().map(s -> new File(s)).collect(Collectors.toList()));
    }

    public static class Options {
        @Parameter
        public List<String> parameters = new ArrayList<>();

        @Parameter(names = { "-depth"}, description = "depth of trees to include")
        public Integer depth = 3;

        @Parameter(names = { "-dir"}, description = "directory")
        public String directory = "./";
    }
}
