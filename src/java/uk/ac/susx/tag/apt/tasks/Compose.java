package uk.ac.susx.tag.apt.tasks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import uk.ac.susx.tag.apt.*;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.RelationIndexer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ds300 on 21/09/2015.
 */
public class Compose {

    final LexiconDescriptor descriptor;
    final Indexer<String> entityIndexer;
    final RelationIndexer relationIndexer;
    final LRUCachedAPTStore lexiconStore;
    final ArrayAPT everythingCounts;

    public Compose(LexiconDescriptor descriptor,
                   Indexer<String> entityIndexer,
                   RelationIndexer relationIndexer,
                   LRUCachedAPTStore lexiconStore,
                   ArrayAPT everythingCounts) {
        this.descriptor = descriptor;
        this.entityIndexer = entityIndexer;
        this.relationIndexer = relationIndexer;
        this.lexiconStore = lexiconStore;
        this.everythingCounts = everythingCounts;
    }


    public static class Options {
        @Parameter
        public List<String> parameters = new ArrayList<>();

        @Parameter(names = {"cache-size"}, description = "The maximum size of the in-memory APT cache")
        public int cacheSize = 100000;

        @Parameter(names = {"method"}, description = "The method of composition to use. One of: sum, sum*")
        public String method = "sum*";

        @Parameter(names = {"pmi"}, description = "Use standard pmi rather than ppmi. (only applies to the sum* method)")
        public boolean pmi = false;
    }

    public static void main(String[] args) {
        Options opts = new Options();
        new JCommander(opts, new String[]{"buns", "jizz", "maloid"});

    }
}
