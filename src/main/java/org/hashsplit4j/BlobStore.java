package org.hashsplit4j;

/**
 * Represents a means of obtaining data for a chunk, indexed by its hash
 *
 * @author brad
 */
public interface BlobStore {
    
    /**
     * Called whenever we find a chunk boundary.
     * 
     * @param hash - the crc of the chunk boundary
     * @param offset - index of first byte
     * @param blob  - bytes in the blob (where bytes are within each integer)
     */
    public void setBlob(long hash, int offset, byte[] bytes);    
    
    byte[] getBlob(Long hash);    
}
