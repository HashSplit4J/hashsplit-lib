/*
 */
package org.hashsplit4j.sfdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.hashsplit4j.store.SimpleFileDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class IndexedSfdb {

    private static final Logger log = LoggerFactory.getLogger(IndexedSfdb.class);
    private final String name;
    private final HashIndex hashIndex;
    private final File keysFile;
    private final File valuesFile;
    

    public IndexedSfdb(String name, HashIndex hashIndex, File keysFile, File valuesFile) {
        this.name = name;
        this.hashIndex = hashIndex;
        this.keysFile = keysFile;
        this.valuesFile = valuesFile;
    }

    public IndexedSfdb.DbItem put(String key, byte[] val) throws FileNotFoundException, IOException {
        long startPos;
        long finishPos;
        try (FileOutputStream fout = new FileOutputStream(valuesFile, true)) { //open in append mode
            FileChannel chan = fout.getChannel();
            startPos = chan.position();
            ByteBuffer bb = ByteBuffer.wrap(val);
            chan.write(bb);
            finishPos = chan.position();
        }

        log.info("save: start={} finish={} key={}", startPos, finishPos, key);
        long keyFilePos;
        try (FileOutputStream fout = new FileOutputStream(keysFile, true)) { //open in append mode
            try (FileChannel chan = fout.getChannel()) {
                String line = key + "," + startPos + "," + finishPos + "\n"; // use text for ease of troubleshooting
                ByteBuffer bb = ByteBuffer.wrap(line.getBytes());
                keyFilePos = chan.position();
                chan.write(bb);
            }
        }

        return null;
    }

    public byte[] get(String key) throws FileNotFoundException, IOException {
        return null;
    }

    public byte[] get(IndexedSfdb.DbItem item) throws FileNotFoundException, IOException {
        return null;
    }


    public class DbItem {

        private final long start;
        private final long finish;

        public DbItem(long start, long finish) {
            this.start = start;
            this.finish = finish;
        }

        byte[] data() throws IOException {
            return IndexedSfdb.this.get(this);
        }
    }
}
