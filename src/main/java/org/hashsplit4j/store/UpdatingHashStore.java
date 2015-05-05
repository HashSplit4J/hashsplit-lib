package org.hashsplit4j.store;

import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;

/**
 *
 * @author brad
 */
public class UpdatingHashStore implements HashStore{

    private final HashStore remoteHashStore;

    public UpdatingHashStore(HashStore remoteHashStore) {
        this.remoteHashStore = remoteHashStore;
    }
        
    
    @Override
    public void setChunkFanout(String hash, List<String> childCrcs, long actualContentLength) {
        if( !remoteHashStore.hasChunk(hash) ) {
            remoteHashStore.setChunkFanout(hash, childCrcs, actualContentLength);
        } else {
            //System.out.println("Remote has existing fanout: " + hash);
        }
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        return remoteHashStore.getChunkFanout(fanoutHash);
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return remoteHashStore.hasChunk(fanoutHash);
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        if( !remoteHashStore.hasFile(hash)) {
            remoteHashStore.setFileFanout(hash, fanoutHashes, actualContentLength);
        }
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        return remoteHashStore.getFileFanout(fileHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return remoteHashStore.hasFile(fileHash);
    }

}
