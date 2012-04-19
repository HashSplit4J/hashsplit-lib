package org.hashsplit4j.api;

import java.util.List;

/**
 * Represents a "fanout" which is a list of hashes which are checksums for
 * portions of a file.
 *  
 * The fanout also contains a content length property so clients can know the content
 * length without having to iterate over all of the chunks
 *
 * @author brad
 */
public interface Fanout {
    long getActualContentLength();
    List<Long> getHashes();
}
