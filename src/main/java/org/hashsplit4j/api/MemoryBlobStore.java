package org.hashsplit4j.api;

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
    public boolean hasBlob(long hash) {
        return mapOfChunks.containsKey(hash);
    }
    
    
    
    @Override
    public byte[] getBlob(long hash) {
        Chunk chunk = mapOfChunks.get(hash);
        if( chunk != null ) {
            return chunk.blob;
        } else {
            return null;
        }
    }
    
    @Override
    public void setBlob(long hash, byte[] bytes) {
        Chunk chunk = new Chunk();
        chunk.crc = hash;
        chunk.start = totalSize;
        chunk.length = bytes.length;        
        chunk.blob = bytes;
        mapOfChunks.put(hash, chunk);
        totalSize+=chunk.length;
        System.out.println("setBlob: " + hash + " size: " + bytes.length);
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
        long start;
        int length;
        byte[] blob;
    }                        
}
