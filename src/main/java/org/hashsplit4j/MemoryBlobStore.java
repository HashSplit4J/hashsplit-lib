package org.hashsplit4j;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author brad
 */
public class MemoryBlobStore implements BlobStore {
    private Map<Long,Chunk> mapOfChunks = new HashMap<Long, Chunk>();    

    private long totalSize;
    
    @Override
    public byte[] getBlob(Long hash) {
        Chunk chunk = mapOfChunks.get(hash);
        if( chunk != null ) {
            return chunk.blob;
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
        chunk.blob = bytes;
        mapOfChunks.put(crc, chunk);
        totalSize+=chunk.length;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public Map<Long, Chunk> getMapOfChunks() {
        return mapOfChunks;
    }
    
    
    /**
     * A chunk just identifies where the data for a given crc is. Useful on the client side
     */
    public class Chunk {
        long crc;
        int start;
        int length;
        byte[] blob;
    }                        
}
