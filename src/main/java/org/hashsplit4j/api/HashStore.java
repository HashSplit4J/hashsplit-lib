package org.hashsplit4j.api;

import java.util.List;

/**
 * Interface for storing and retrieving the result of hash splitting operations.
 *
 * There are 3 types of information stored: blob: an array of bytes which is a
 * contiguous portion of a file chunk: a blob and its hash (ie SHA1) and offset
 * information, so the chunk can be located in the file in which is was found
 * fanout: a hash value to use as a grouping of chunks. The given hash is the
 * hash of all of the contained chunks
 *
 * @author brad
 */
public interface HashStore {

    /**
     *
     *
     * @param hash - the hex encoded form of the SHA1 hash
     * @param blobHashes - list of hex encoded SHA1 hashes in this fanout
     * @param actualContentLength
     */
    void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength);

    void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength);

    Fanout getFileFanout(String fileHash);

    Fanout getChunkFanout(String fanoutHash);

    boolean hasChunk(String fanoutHash);

    boolean hasFile(String fileHash);
}
