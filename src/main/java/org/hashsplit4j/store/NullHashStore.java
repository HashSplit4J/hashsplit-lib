package org.hashsplit4j.store;

import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;

/**
 *
 * @author brad
 */
public class NullHashStore implements HashStore{



    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {

    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {

    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return false;
    }

    @Override
    public boolean hasFile(String fileHash) {
        return false;
    }

}
