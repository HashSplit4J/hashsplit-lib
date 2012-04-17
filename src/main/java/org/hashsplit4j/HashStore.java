package org.hashsplit4j;

import java.util.List;

/**
 *
 * @author brad
 */
public interface HashStore {
    /**
     * Called whenever we find a chunk boundary.
     * 
     * @param crc - the crc of the chunk boundary
     * @param start - index of first byte
     * @param finish - index of last byte
     * @param blob  - bytes in the blob (where bytes are within each integer)
     */
    public void onChunk(long crc, int start, int finish, byte[] bytes);
    public void onFanout(long crc, List<Long> childCrcs);

    public List<Long> getCrcsFromFanout(Long fanoutCrc);

    public byte[] getBlob(Long crc);
    
}
