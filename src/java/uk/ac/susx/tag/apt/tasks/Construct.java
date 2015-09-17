package uk.ac.susx.tag.apt.tasks;

import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.ConllReader;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

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


    final ConcurrentLinkedQueue<File> files;
    final ConcurrentLinkedQueue<List<String[]>> sentences;

    final ExecutorService sentenceExtractors;
    final ExecutorService sentenceConsumers;

    final AtomicInteger numSentencesProcessed = new AtomicInteger(0);

    class SetenceExtractor implements Runnable {
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

            if (entity[2] != null && entity[2].length() > 0) {
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
}
