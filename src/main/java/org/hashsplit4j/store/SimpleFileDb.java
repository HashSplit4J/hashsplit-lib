package org.hashsplit4j.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just class to save byte[] to a file with a key for efficient lookup
 *
 * @author brad
 */
public class SimpleFileDb {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDb.class);

    private final File keysFile;
    private final File valuesFile;

    private final Map<String, DbItem> mapOfItems = new HashMap<>();

    public SimpleFileDb(File keysFile, File valuesFile) {
        this.keysFile = keysFile;
        this.valuesFile = valuesFile;
    }

    public void init() throws FileNotFoundException, IOException {
        if (keysFile.exists()) {
            try (FileInputStream fin = new FileInputStream(keysFile)) {
                InputStreamReader r1 = new InputStreamReader(fin);
                BufferedReader reader = new BufferedReader(r1);
                String line = reader.readLine();
                while (line != null) {
                    parseAndAdd(line);
                    line = reader.readLine();
                }
            }
        }
    }

    public int size() {
        return mapOfItems.size();
    }

    Map<String, DbItem> getMapOfItems() {
        return mapOfItems;
    }

    public DbItem put(String key, byte[] val) throws FileNotFoundException, IOException {
        if (mapOfItems.containsKey(key)) {
            throw new RuntimeException("Key " + key + " is already present");
        }

        long startPos;
        long finishPos;
        try (FileOutputStream fout = new FileOutputStream(valuesFile, true)) { //open in append mode
            FileChannel chan = fout.getChannel();
            startPos = chan.position();
            ByteBuffer bb = ByteBuffer.wrap(val);
            chan.write(bb);
            finishPos = chan.position();
        }
        DbItem dbItem = new DbItem(startPos, finishPos);

        log.info("save: start={} finish={} key={}", startPos, finishPos, key);
        try (FileOutputStream fout = new FileOutputStream(keysFile, true)) { //open in append mode
            try (FileChannel chan = fout.getChannel()) {
                String line = key + "," + startPos + "," + finishPos + "\n"; // use text for ease of troubleshooting
                ByteBuffer bb = ByteBuffer.wrap(line.getBytes());
                chan.write(bb);
            }
        }

        mapOfItems.put(key, dbItem);

        return dbItem;

    }

    public byte[] get(String key) throws FileNotFoundException, IOException {
        DbItem item = mapOfItems.get(key);
        if (item == null) {
            return null;
        }
        return get(item);
    }

    public byte[] get(DbItem item) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(keysFile, "r");
        try (FileChannel chan = raf.getChannel()) {
            int size = (int) (item.finish - item.start);
            ByteBuffer bb = ByteBuffer.allocate(size);
            chan.position(item.start).read(bb);
            bb.flip();
            byte[] arr = bb.array();
            return arr;
        }
    }

    private void parseAndAdd(String line) {
        String[] arr = line.split(",");
        if (arr.length != 3) {
            log.info("Invalid line, not 3 parts: {}", line);
        } else {
            String key = arr[0];
            long start = Long.valueOf(arr[1]);
            long finish = Long.valueOf(arr[2]);
            DbItem dbItem = new DbItem(start, finish);
            mapOfItems.put(key, dbItem);
        }
    }

    public class DbItem {

        private final long start;
        private final long finish;

        public DbItem(long start, long finish) {
            this.start = start;
            this.finish = finish;
        }

        byte[] data() throws IOException {
            return SimpleFileDb.this.get(this);
        }
    }
}
