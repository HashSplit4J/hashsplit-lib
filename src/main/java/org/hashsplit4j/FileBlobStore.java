package org.hashsplit4j;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Like a MemoryBlobStore, except instead of storing blobs in memory it
 * just holds their offset and gets the data from the underlying file
 *
 * @author brad
 */
public class FileBlobStore implements BlobStore{
    
    private Map<Long,Chunk> mapOfChunks = new HashMap<Long, Chunk>();    

    private final File file;

    private RandomAccessFile raf;
    
    private long totalSize;
    
    public FileBlobStore(File file) {
        this.file = file;
    }
       
    @Override
    public byte[] getBlob(Long hash) {
        if( raf == null ) {
            throw new IllegalStateException("File has not been opened, please call openForRead (and then close when finished!");
        }
        Chunk chunk = mapOfChunks.get(hash);
        if( chunk != null ) {
            try {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                raf.seek(chunk.start);
                readBytes(raf, bout, chunk.length);
                return bout.toByteArray();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return null;
        }
    }
    
    @Override
    public void setBlob(long crc, int start, byte[] bytes) {
        Chunk chunk = new Chunk();
        chunk.crc = crc;
        chunk.start = start;
        chunk.length = bytes.length;        
        mapOfChunks.put(crc, chunk);
        totalSize += bytes.length;
    }

    private void readBytes(RandomAccessFile raf, OutputStream bout, int length) throws IOException {
        byte[] arr = new byte[length];
        int s = raf.read(arr, 0, length);
        while( s >= 0 ) {
            bout.write(arr, 0, s);
            s = raf.read(arr, 0, length);
        }
    }

    public class Chunk {
        long crc;
        int start;
        int length;
    }    
    
    public void openForRead() throws FileNotFoundException {
        raf = new RandomAccessFile(file, "r");
    }
        
    public void close() {
        if( raf != null ) {
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
