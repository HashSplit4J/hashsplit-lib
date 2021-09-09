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
import java.util.Arrays;
import org.hashsplit4j.store.SimpleFileDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps a key to a hashed position in the index file, which points to the head
 * of a linked list in a second keys file
 *
 * @author brad
 */
public class HashIndex {

    private static final Logger log = LoggerFactory.getLogger(HashIndex.class);

    private final File hashFile;
    private final File keysFile;
    private final int expectedItems;
    private boolean allowDups;

    public HashIndex(File indexFile, File keysListFile, int expectedItems, boolean allowDups) {
        this.hashFile = indexFile;
        this.keysFile = keysListFile;
        this.expectedItems = expectedItems;
        this.allowDups = allowDups;
    }

    public long getDbPos(String key) throws IOException {
        int pos = calcHashPosition(key);    // byte position, needs to allow room for 8 bytes for each key
        try (RandomAccessFile raf = new RandomAccessFile(keysFile, "r")) {
            try (FileChannel hashFileChan = raf.getChannel()) {
                long p = pos;
                while (true) {
                    KeyEntry keyEntry = readKeyFromHashFile(hashFileChan, pos);
                    if (keyEntry.hasKey()) {
                        if (keyEntry.key.equals(key)) {
                            return keyEntry.keysFilePos;
                        } else if( keyEntry.nextKeyPosition != null) {
                            p = keyEntry.nextKeyPosition;
                        } else {
                            // reached tail, not found
                            return -1;
                        }
                    } else {
                        return -1;
                    }
                }
            }
        }
    }

    public IndexedSfdb.DbItem put(String key, long dbPosition) throws FileNotFoundException, IOException {

        int pos = calcHashPosition(key);    // byte position, needs to allow room for 8 bytes for each key

        try (FileOutputStream fout = new FileOutputStream(hashFile, false)) { //open in append mode
            try (FileChannel hashFileChan = fout.getChannel()) {
                if (pos > hashFileChan.size()) {
                    // no existing key at this pos
                    long newKeyPos = writeNewKey(key, dbPosition, null);
                    writeHash(pos, newKeyPos, hashFileChan);
                } else {
                    KeyEntry keyEntry = readKeyFromHashFile(hashFileChan, pos);
                    long newKeyPos;
                    if (!keyEntry.hasKey()) {
                        newKeyPos = writeNewKey(key, dbPosition, null);
                    } else {
                        // collision or duplicate.
                        // insert into keys file, then update with the new head
                        newKeyPos = writeNewKey(key, dbPosition, keyEntry.keysFilePos);
                    }
                    writeHash(pos, newKeyPos, hashFileChan);
                }
            }
        }

        return null;
    }

    private KeyEntry readKeyFromHashFile(final FileChannel hashFileChan, int pos) throws IOException {
        hashFileChan.position(pos);
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
        hashFileChan.read(bb);
        bb.flip();
        int keyLength = bb.getInt();

        bb = ByteBuffer.allocate(keyLength);
        hashFileChan.read(bb);
        bb.flip();
        byte[] keyBytes = bb.array();
        String key = new String(keyBytes);

        bb = ByteBuffer.allocate(Long.BYTES);
        hashFileChan.read(bb);
        bb.flip();
        long keysFilePos = bb.getLong();

        bb = ByteBuffer.allocate(Long.BYTES);
        hashFileChan.read(bb);
        bb.flip();
        long nextKeyPosition = bb.getLong();
        Long _nextKeyPosition = nextKeyPosition == 0 ? null : nextKeyPosition;

        return new KeyEntry(key, keysFilePos, _nextKeyPosition);
    }

    private class KeyEntry {

        private final String key;
        private final long keysFilePos;
        private final Long nextKeyPosition;

        public KeyEntry(String key, long keysFilePos, Long nextKeyPosition) {
            this.key = key;
            this.keysFilePos = keysFilePos;
            this.nextKeyPosition = nextKeyPosition;
        }

        public boolean hasKey() {
            return keysFilePos > 0;
        }

    }

    /**
     * returns a byte position of the key (using its hashCode)
     *
     * @param key
     * @return
     */
    private int calcHashPosition(String key) {
        int h = key.hashCode(); // compress this onto address space of size expectedItems, but..
        // need to return a byte location so will x 8
        int compressedPos = h % expectedItems;
        return compressedPos * Long.BYTES;
    }

    /**
     * Writes the key and dbPos (ie of the key in the main db file) into
     * keysFile, and return this address
     * <br>
     * Implements a singly linked list, with the head being the most recently
     * added key
     *
     * @param key - the value being indexed
     * @param dbPosition - the location of this row in the main DB
     * @param nextKeyPosition - if not null, is the next key in the linked list
     * (this will be a previosly existing key)
     */
    private long writeNewKey(String key, long dbPosition, Long nextKeyPosition) throws FileNotFoundException, IOException {

        //TODO: need to write the key, which is variable length, so need to write the length
        try (FileOutputStream fout = new FileOutputStream(keysFile, true)) { //open in append mode
            try (FileChannel keysChan = fout.getChannel()) {
                byte[] keyBytes = key.getBytes();
                ByteBuffer bb = ByteBuffer.allocate(keyBytes.length + Integer.BYTES + Long.BYTES * 2);
                bb.putInt(keyBytes.length);
                bb.put(keyBytes);

                long newKeyPos = keysChan.position();
                long nextKey = nextKeyPosition == null ? 0 : nextKeyPosition;
                bb.putLong(dbPosition);
                bb.putLong(nextKey);
                keysChan.write(bb);
                return newKeyPos;
            }
        }
    }

    /**
     * Create or update the value in the file
     *
     * @param pos - position within the hash file to write to
     * @param newKeyPos - position within the key file to save, this is the head
     * of the linked list of keys with this hash
     * @param chan - chan to save to
     */
    private void writeHash(int pos, long newKeyPos, FileChannel chan) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        bb.putLong(newKeyPos);
        if (pos > chan.size()) {
            long length = pos - chan.size();
            fill(chan, chan.size(), length);
        }
        chan.position(pos);
        chan.write(bb);
    }

    public static void fill(final FileChannel fileChannel, final long position, final long length) throws IOException {
        int BLOCK_SIZE = 1024;
        final byte[] filler = new byte[BLOCK_SIZE];
        byte b = 0;
        Arrays.fill(filler, b);

        final ByteBuffer byteBuffer = ByteBuffer.wrap(filler);
        fileChannel.position(position);

        final int blocks = (int) (length / BLOCK_SIZE);
        final int blockRemainder = (int) (length % BLOCK_SIZE);

        for (int i = 0; i < blocks; i++) {
            byteBuffer.position(0);
            fileChannel.write(byteBuffer);
        }

        if (blockRemainder > 0) {
            byteBuffer.position(0);
            byteBuffer.limit(blockRemainder);
            fileChannel.write(byteBuffer);
        }

    }
}
