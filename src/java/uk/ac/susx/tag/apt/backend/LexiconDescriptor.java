package uk.ac.susx.tag.apt.backend;

import uk.ac.susx.tag.apt.RGraph;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.io.*;
import java.util.concurrent.locks.ReentrantLock;

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
        File f = file(relationIndexFilename);
        if (f.exists()) {
            return RelationIndexer.from(IO.getIndexerMapFromTSVFile(f));
        } else {
            return RelationIndexer.empty();
        }
    }


}
