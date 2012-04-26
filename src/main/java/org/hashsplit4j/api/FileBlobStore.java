package org.hashsplit4j.api;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

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
    public static byte[] readBytes(RandomAccessFile raf, long offset, int length, long expectedHash) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();        
        raf.seek(offset);
        readBytes(raf, bout, length, expectedHash);
        return bout.toByteArray();

    }

    private static void readBytes(RandomAccessFile raf, OutputStream bout, int length, long expectedHash) throws IOException {
        byte[] arr = new byte[length];
        CRC32 blobCrc = new CRC32();
        int s = raf.read(arr, 0, length);
        while (s > 0) {
            blobCrc.update(arr, 0, s);
            bout.write(arr, 0, s);
            length = length - s; // read only up to remaining expected bytes
            s = raf.read(arr, 0, length);
        }
        long actualCrc = blobCrc.getValue();
        if( actualCrc != expectedHash ) {
            throw new RuntimeException("Actual CRC of blob does not match expected");
        }
    }    
    
    private Map<Long, Chunk> mapOfChunks = new HashMap<Long, Chunk>();
    private final File file;
    private RandomAccessFile raf;
    private long totalSize;

    public FileBlobStore(File file) {
        this.file = file;
    }

    @Override
    public boolean hasBlob(long hash) {
        return mapOfChunks.containsKey(hash);
    }

    public File getFile() {
        return file;
    }

    @Override
    public byte[] getBlob(long hash) {
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
    public void setBlob(long crc, byte[] bytes) {
        Chunk chunk = new Chunk();
        chunk.crc = crc;
        chunk.start = totalSize;
        chunk.length = bytes.length;
        mapOfChunks.put(crc, chunk);
        totalSize += bytes.length;
    }


    public class Chunk {

        long crc;
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
