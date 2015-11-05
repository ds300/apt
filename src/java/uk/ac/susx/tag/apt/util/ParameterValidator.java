package uk.ac.susx.tag.apt.util;

import com.beust.jcommander.ParameterException;

import java.io.File;
import java.util.Collection;

/**
 * Checks input parameters
 * Created by mmb28 on 05/11/2015.
 */
public class ParameterValidator {

    public static void fileExists(File f) throws ParameterException {
        if (!f.exists())
            throw new ParameterException("File " + f + " does not exist");
    }

    public static void fileExists(String file) throws ParameterException {
        fileExists(new File(file));
    }

    public static void filesExist(Collection files) throws ParameterException {
        for (Object f : files)
            fileExists(f.toString());
    }


    public static void atLeast(Collection<String> params, int minLength) {
        if (params.size() < minLength)
            throw new ParameterException("Expected at least " + minLength + " params, got " + params.size());
    }

    /**
     * Checks if the specified directory looks like a lexicon, i.e. exists and has all the required files
     * @param dir
     */
    public static void isLexicon(String dir) {
        File f = new File(dir);
        if (!f.exists())
            throw new ParameterException("File " + dir + " does not exist");
        else if (!f.isDirectory())
            throw new ParameterException("File " + dir + " is not a directory");

        String[] requiredFiles = new String[]{"everything-counts.apt.gz", "entity-index.tsv.gz", "relation-index.tsv.gz"};
        for (String s : requiredFiles) {
            if (!(new File(dir, s).exists())) {
                throw new ParameterException(dir + " does not appear to be a valid lexicon");
            }
        }
    }
}
