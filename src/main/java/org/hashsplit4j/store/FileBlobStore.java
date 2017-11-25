package org.hashsplit4j.store;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.bouncycastle.crypto.Digest;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.api.Parser;

/**
 * Like a MemoryBlobStore, except instead of storing blobs in memory it just
 * holds their offset and gets the data from the underlying file
 *
 * @author brad
 */
public class FileBlobStore implements BlobStore {

    /**
     * Attempts to read the block of bytes in the file at the given offset
     * and of the given length. Generated a CRC of the read bytes and compares with
     * that expected. If they differ an IOException is thrown
     * 
     * @param raf
     * @param offset
     * @param length
     * @param expectedHash
     * @return
     * @throws IOException 
     */
    public static byte[] readBytes(RandomAccessFile raf, long offset, int length, String expectedHash) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();        
        raf.seek(offset);
        readBytes(raf, bout, length, expectedHash);
        return bout.toByteArray();

    }

    private static void readBytes(RandomAccessFile raf, OutputStream bout, int length, String expectedHash) throws IOException {
        byte[] arr = new byte[length];
        Digest blobCrc = Parser.getCrypt();
        int s = raf.read(arr, 0, length);
        while (s > 0) {
            blobCrc.update(arr, 0, s);
            bout.write(arr, 0, s);
            length = length - s; // read only up to remaining expected bytes
            s = raf.read(arr, 0, length);
        }
        String actualCrc = Parser.toHex(blobCrc);
        if( !actualCrc.equals(expectedHash) ) {
            throw new RuntimeException("Actual CRC of blob does not match expected");
        }
    }    
    
    private Map<String, Chunk> mapOfChunks = new HashMap<String, Chunk>();
    private final File file;
    private RandomAccessFile raf;
    private long totalSize;

    public FileBlobStore(File file) {
        this.file = file;
    }

    @Override
    public boolean hasBlob(String hash) {
        return mapOfChunks.containsKey(hash);
    }

    public File getFile() {
        return file;
    }

    @Override
    public byte[] getBlob(String hash) {
        if (raf == null) {
            throw new IllegalStateException("File has not been opened, please call openForRead (and then close when finished!");
        }
        Chunk chunk = mapOfChunks.get(hash);
        if (chunk != null) {
            try {
                byte[] arr = readBytes(raf, chunk.start, chunk.length, hash);
                return arr;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return null;
        }
    }

    @Override
    public void setBlob(String crc, byte[] bytes) {
        Chunk chunk = new Chunk();
        chunk.crc = crc;
        chunk.start = totalSize;
        chunk.length = bytes.length;
        mapOfChunks.put(crc, chunk);
        totalSize += bytes.length;
    }


    public class Chunk {

        String crc;
        long start;
        int length;
    }

    public void openForRead() throws FileNotFoundException {
        raf = new RandomAccessFile(file, "r");
    }

    public void close() {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ex) {
                // ignore
            }
        }
    }

    public long getTotalSize() {
        return totalSize;
    }
}
