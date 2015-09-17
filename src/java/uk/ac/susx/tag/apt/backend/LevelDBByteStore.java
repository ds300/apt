package uk.ac.susx.tag.apt.backend;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import uk.ac.susx.tag.apt.PersistentKVStore;
import uk.ac.susx.tag.apt.Util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ds300 on 17/09/2015.
 */
public class LevelDBByteStore implements PersistentKVStore<Integer, byte[]> {
    private final DB db;

    public static LevelDBByteStore fromDescriptor(LexiconDescriptor descriptor) throws IOException {
        return new LevelDBByteStore(descriptor.directory);
    }

    public LevelDBByteStore(File dir) throws IOException {
        this(dir, new Options().createIfMissing(true));
    }

    public LevelDBByteStore(File dir, Options opts) throws IOException {
        DB db;
        try {
            db = JniDBFactory.factory.open(dir, opts);
        } catch (Exception e) {
            System.err.println("can't open native leveldb, using pure java version");
            db = Iq80DBFactory.factory.open(dir, opts);
        }
        this.db = db;
    }

    @Override
    public void put(Integer key, byte[] value) throws IOException {
        db.put(Util.int2bytes(key), value);
    }

    @Override
    public synchronized boolean atomicCAS(Integer key, byte[] expected, byte[] value) throws IOException {
        if (Arrays.equals(expected, get(key))) {
            this.put(key, value);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void remove(Integer key) throws IOException {
        db.delete(Util.int2bytes(key));
    }

    @Override
    public synchronized byte[] get(Integer key) throws IOException {
        byte[] stuff = db.get(Util.int2bytes(key));
        if (stuff != null && stuff.length > 0) {
            return stuff;
        } else {
            return null;
        }
    }

    @Override
    public boolean containsKey(Integer key) throws IOException {
        return get(key) != null;
    }

    @Override
    public synchronized void close() throws IOException {
        db.close();
    }

    @Override
    public Iterator<Map.Entry<Integer, byte[]>> iterator() {
        final LevelDBByteStore that = this;
        synchronized (that) {
            final DBIterator delegate = db.iterator();
            delegate.seekToFirst();

            return new Iterator<Map.Entry<Integer, byte[]>>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Map.Entry<Integer, byte[]> next() {
                    synchronized (that) {
                        Map.Entry<byte[], byte[]> e =  delegate.next();
                        if (e != null) {
                            final Integer key = Util.bytes2int(e.getKey());
                            final byte[] val = e.getValue();
                            return new Map.Entry<Integer, byte[]>() {
                                @Override
                                public Integer getKey() {
                                    return key;
                                }

                                @Override
                                public byte[] getValue() {
                                    return val;
                                }

                                @Override
                                public byte[] setValue(byte[] value) {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        } else {
                            return null;
                        }
                    }
                }
            };
        }
    }
}
