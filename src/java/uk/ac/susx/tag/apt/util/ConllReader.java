package uk.ac.susx.tag.apt.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Created by ds300 on 17/09/2015.
 */
public class ConllReader<T> implements AutoCloseable, Iterable<List<T>> {
    private final BufferedReader reader;
    private final Function<String[], T> entryTransformer;

    public static <T> ConllReader<T> from(Reader reader, Function<String[], T> entryTransformer) {
        return new ConllReader<>(reader, entryTransformer);
    }

    public static ConllReader<String[]> from(Reader reader) {
        return new ConllReader<>(reader, x -> x);
    }

    private ConllReader(Reader reader, Function<String[], T> entryTransformer) {
        this.reader = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
        this.entryTransformer = entryTransformer;
    }


    private List<T> nextSentence() throws IOException {
        List<T> result = new ArrayList<>();

        String currentLine = null;
        while ((currentLine = reader.readLine()) != null && !currentLine.equals("")) {
            result.add(entryTransformer.apply(currentLine.split("\t")));
        }

        if (currentLine == null && result.size() == 0) {
            return null;
        } else {
            return result;
        }
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    @Override
    public Iterator<List<T>> iterator() {

        try {
            final AtomicReference<List<T>> next = new AtomicReference<>(nextSentence());
            return new Iterator<List<T>>() {
                @Override
                public synchronized boolean hasNext() {
                    return next.get() != null;
                }

                @Override
                public synchronized List<T> next() {
                    try {
                        if (hasNext()) {
                            List<T> nextValue = next.get();
                            next.set(nextSentence());
                            return nextValue;
                        } else {
                            return null;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
