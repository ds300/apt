package uk.ac.susx.tag.apt.backend;

import clojure.lang.*;
import uk.ac.susx.tag.apt.AccumulativeLazyAPT;
import uk.ac.susx.tag.apt.ArrayAPT;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.IndexerImpl;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by ds300 on 11/09/2015.
 */
public class LexiconDescriptor {
    public final File directory;
    public final String entityIndexFilename;
    public final String relationIndexFilename;
    public final String pathCountsFilename;
    public final String sumFilename;
    public final String everythingCountsFilename;

    /**
     * Makes a new lexicon descriptor with default values
     * @param dir
     * @return
     */
    public static LexiconDescriptor from(String dir) {
        return from(new File(dir));
    }

    /**
     * Makes a new lexcion descriptor with default values
     * @param dir
     * @return
     */
    public static LexiconDescriptor from(File dir) {
        return new LexiconDescriptor(dir);
    }

    /**
     * Makes a new lexicon descriptor with default values
     * @param directory
     */
    public LexiconDescriptor(File directory) {
        this.directory = directory;
        this.entityIndexFilename = "entity-index.tsv.gz";
        this.relationIndexFilename = "relation-index.tsv.gz";
        this.pathCountsFilename = "path-counts.tsv.gz";
        this.sumFilename = "sum";
        this.everythingCountsFilename = "everything-counts.apt.gz";
    }

    /**
     * Makes a new lexicon descriptor
     * @param directory
     * @param entityIndexFilename
     * @param relationIndexFilename
     * @param pathCountsFilename
     * @param sumFilename
     * @param everythingCountsFilename
     */
    public LexiconDescriptor(File directory,
                              String entityIndexFilename,
                              String relationIndexFilename,
                              String pathCountsFilename,
                              String sumFilename,
                              String everythingCountsFilename) {
        this.directory = directory;
        this.entityIndexFilename = entityIndexFilename;
        this.relationIndexFilename = relationIndexFilename;
        this.pathCountsFilename = pathCountsFilename;
        this.sumFilename = sumFilename;
        this.everythingCountsFilename = everythingCountsFilename;
    }

    public File file(String filename) {
        return new File(directory, filename);
    }


    public RelationIndexer getRelationIndexer() throws IOException {
        return ifFileExists(relationIndexFilename,
                f -> RelationIndexer.from(IO.getIndexerMapFromTSVFile(f)),
                f -> RelationIndexer.empty());
    }

    public IndexerImpl getEntityIndexer() throws IOException {
        return ifFileExists(entityIndexFilename,
                f -> IndexerImpl.from(IO.getIndexerMapFromTSVFile(f)),
                f -> IndexerImpl.empty());
    }

    public ArrayAPT getEverythingCounts () throws IOException {
        return ifFileExists(everythingCountsFilename,
                f -> ArrayAPT.fromByteArray(IO.getBytes(f)),
                f -> ArrayAPT.factory.empty());
    }

    public void storeEverythingCounts (AccumulativeLazyAPT apt) throws IOException {
        ifFileExistsDo(everythingCountsFilename,
                f -> IO.putBytes(f, apt.mergeIntoExisting(IO.getBytes(f))),
                f -> IO.putBytes(f, apt.toByteArray()));
    }

    private <T> T ifFileExists(String filename, FunctionThatThrows<File, T> then, FunctionThatThrows<File, T> otherwise) throws IOException {
        File f = file(filename);
        if (f.exists()) {
            return then.apply(f);
        } else {
            return otherwise.apply(f);
        }
    }
    private void ifFileExistsDo(String filename, ConsumerThatThrows<File> then, ConsumerThatThrows<File> otherwise) throws IOException {
        File f = file(filename);
        if (f.exists()) {
            then.consume(f);
        } else {
            otherwise.consume(f);
        }
    }

    private void storeIndex(String filename, PersistentHashMap val2idx) throws IOException {
        try (final Writer out = IO.writer(file(filename))) {
            val2idx.kvreduce(new AFn() {
                @Override
                public Object invoke(Object arg1, Object arg2, Object arg3) {
                    try {
                        out.write(arg2.toString() + "\t" + arg3.toString() + "\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            }, null);
        }
    }

    public void storeEntityIndexer(IndexerImpl entityIndexer) throws IOException {
        storeIndex(entityIndexFilename, (PersistentHashMap) entityIndexer.getState().val2idx);
    }

    public void storeRelationIndexer(RelationIndexer relationIndexer) throws IOException {
        storeIndex(relationIndexFilename, (PersistentHashMap) relationIndexer.getState().val2idx);
    }

    @FunctionalInterface
    private interface FunctionThatThrows<T, R> {
        R apply(T t) throws IOException;
    }

    @FunctionalInterface
    private interface ConsumerThatThrows<T> {
        void consume(T t) throws IOException;
    }
}
