package uk.ac.susx.tag.apt.tasks;

import uk.ac.susx.tag.apt.ArrayAPT;
import uk.ac.susx.tag.apt.util.IO;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by ds300 on 28/10/2015.
 */
public class Vectorize {
    public static void main(String[] args) throws IOException {
        for (String inputFileName : args) {
            File inputFile = new File(inputFileName);
            File outputFile = new File(inputFile.getParentFile(), inputFile.getName() + "-vectorized");

            ArrayAPT apt = ArrayAPT.factory.fromByteArray(IO.getBytes(inputFile));
            try (Writer out = IO.writer(outputFile)) {
                Vectors.writeVector(apt, out, null, null, false, false, 0);
                out.write('\n');
            }
        }
    }
}
