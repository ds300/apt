package uk.ac.susx.tag.apt.util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by ds300 on 11/09/2015.
 */
public class IO {
    public static BufferedReader reader(String path) throws IOException {
        return reader(new File(path));
    }
    public static BufferedReader reader(File path) throws IOException {
        InputStream in = new FileInputStream(path);
        if (path.getName().endsWith(".gz")) {
            in = new GZIPInputStream(in);
        }
        return new BufferedReader(new InputStreamReader(in));
    }

    public static BufferedWriter writer(String path) throws IOException {
        return writer(new File(path));
    }
    public static BufferedWriter writer(File file) throws IOException {
        OutputStream out = new FileOutputStream(file);
        if (file.getName().endsWith(".gz")) {
            out = new GZIPOutputStream(out);
        }
        return new BufferedWriter(new OutputStreamWriter(out));
    }

    public static Map<String, Integer> getIndexerMapFromTSVFile(String path) throws IOException {
        return getIndexerMapFromTSVFile(new File(path));
    }
    public static Map<String, Integer> getIndexerMapFromTSVFile(File file) throws IOException {
        Map<String, Integer> result = new HashMap<>();

        if (file.exists()) {
            try (BufferedReader in = IO.reader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    if (line.length() != 0) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            result.put(parts[0], new Integer(parts[1]));
                        } else {
                            throw new Error("expecting only tuples. got: '"+line+"'");
                        }
                    }
                }
            }
        }

        return result;
    }

    public static void writeIndexerMapAsTSVFile(String filename, Map<String, Long> indexerMap) throws IOException {
        writeIndexerMapAsTSVFile(new File(filename), indexerMap);
    }
    public static void writeIndexerMapAsTSVFile(File file, Map<String, Long> indexerMap) throws IOException {
        try (Writer out = writer(file)) {
            for (Map.Entry<String, Long> e : indexerMap.entrySet()) {
                out.write(e.getKey());
                out.write("\t");
                out.write(e.getValue().toString());
                out.write("\n");
            }
        }
    }
}
