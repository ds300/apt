package uk.ac.susx.tag.apt.backend;

import uk.ac.susx.tag.apt.AccumulativeLazyAPT;
import uk.ac.susx.tag.apt.ArrayAPT;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.Indexer;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.File;
import java.io.IOException;

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

    public Indexer getEntityIndexer() throws IOException {
        return ifFileExists(entityIndexFilename,
                f -> Indexer.from(IO.getIndexerMapFromTSVFile(f)),
                f -> Indexer.empty());
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

    @FunctionalInterface
    private interface FunctionThatThrows<T, R> {
        R apply(T t) throws IOException;
    }

    @FunctionalInterface
    private interface ConsumerThatThrows<T> {
        void consume(T t) throws IOException;
    }
}
