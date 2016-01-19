package uk.ac.susx.tag.apt.tasks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import uk.ac.susx.tag.apt.ArrayAPT;
import uk.ac.susx.tag.apt.backend.LexiconDescriptor;
import uk.ac.susx.tag.apt.util.IO;
import uk.ac.susx.tag.apt.util.ParameterValidator;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ds300 on 28/10/2015.
 */
public class ComposedVectors {
    public static class Options {
        @Parameter
        public List<String> files = new ArrayList<>();

        @Parameter(names = {"-compact"}, description = "If true, path indices will not be resolved into lemmas")
        public boolean compact = false;

        @Override
        public String toString() {
            return "Options{" +
                    "files=" + files +
                    ", compact=" + compact +
                    '}';
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting convertion of composed APTs to vectors");
        Options opts = new Options();
        new JCommander(opts, args);
        System.out.println(opts);
        ParameterValidator.isLexicon(opts.files.get(0));
        LexiconDescriptor descriptor = LexiconDescriptor.from(opts.files.get(0));

        ParameterValidator.filesExist(opts.files.subList(1, opts.files.size()).stream().map(Object::toString).collect(Collectors.toList()));
        for (String inputFileName : opts.files.subList(1, opts.files.size())) {
            File inputFile = new File(inputFileName);
            File outputFile = new File(inputFile.getParentFile(), inputFile.getName() + "-vectorized");

            ArrayAPT apt = ArrayAPT.factory.fromByteArray(IO.getBytes(inputFile));
            try (Writer out = IO.writer(outputFile)) {
                if (opts.compact)
                    Vectors.writeVector(apt, out, false, 0);
                else
                    Vectors.writeVector(apt, out, descriptor.getEntityIndexer(), descriptor.getRelationIndexer(), false, true, 0);
                out.write('\n');
            }
        }
    }
}
