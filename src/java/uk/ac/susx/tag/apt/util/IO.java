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
        return new BufferedReader(new InputStreamReader(inputStream(path)));
    }

    public static BufferedWriter writer(String path) throws IOException {
        return writer(new File(path));
    }
    public static BufferedWriter writer(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(outputStream(file)));
    }

    public static InputStream inputStream(File file) throws IOException {
        InputStream in = new FileInputStream(file);
        if (file.getName().endsWith(".gz")) {
            in = new GZIPInputStream(in);
        }
        return in;
    }

    public static OutputStream outputStream(File file) throws IOException {
        OutputStream out = new FileOutputStream(file);
        if (file.getName().endsWith(".gz")) {
            out = new GZIPOutputStream(out);
        }
        return out;
    }

    public static byte[] getBytes(File file) throws IOException {
        try (InputStream in = inputStream(file)) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            byte[] buf = new byte[1024 * 1024];

            int numBytesRead;

            while ((numBytesRead = in.read(buf)) != -1) {
                bytes.write(buf, 0, numBytesRead);
            }

            return bytes.toByteArray();
        }
    }

    public static void putBytes(File file, byte[] bytes) throws IOException {
        try (OutputStream out = outputStream(file)) {
            out.write(bytes);
        }
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
