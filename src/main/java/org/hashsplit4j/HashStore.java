package org.hashsplit4j;

import java.util.List;

/**
 * Interface for storing and retrieving the result of hash splitting
 * operations.
 * 
 * There are 3 types of information stored:
 * blob: an array of bytes which is a contiguous portion of a file
 * chunk: a blob and its hash (ie CRC) and offset information, so the chunk can be located in the file in which is was found
 * fanout: a hash value to use as a grouping of chunks. The given hash is the hash of all of the contained chunks
 *
 * @author brad
 */
public interface HashStore {

    public void onFanout(long hash, List<Long> childCrcs);

    public List<Long> getFanout(Long fanoutHash);      
}
