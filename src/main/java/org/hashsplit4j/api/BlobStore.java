package org.hashsplit4j.api;

/**
 * Represents a means of obtaining data for a chunk, indexed by its hash
 * 
 * Note that it is up to a BlobStore implementation to prevent duplication,
 * if indeed it does. Implementations should usually check for a blob's existence
 * in the setBlob method and ignore the new blob if it already exists
 *
 * @author brad
 */
public interface BlobStore {
    
    /**
     * Called whenever we find a chunk boundary.
     * 
     * @param hash - the crc of the chunk boundary
     * @param blob  - bytes in the blob (where bytes are within each integer)
     */
    void setBlob(long hash, byte[] bytes);    
    
    byte[] getBlob(long hash);    
    
    boolean hasBlob(long hash);
    
}
