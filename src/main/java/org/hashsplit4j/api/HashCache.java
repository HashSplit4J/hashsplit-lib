package org.hashsplit4j.api;

/**
 * A hash cache is just to optimise knowledge of the existence of hashes
 * in remote systems, its not for actually storing the data associated
 * with the hashes
 *
 * @author brad
 */
public interface HashCache {
    boolean hasHash(final long hash);
    
    void setHash(long hash);
}
