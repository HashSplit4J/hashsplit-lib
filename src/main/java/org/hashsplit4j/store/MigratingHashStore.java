package org.hashsplit4j.store;

import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;

/**
 * Attempts to get from the new store. If that fails will get from the old store,
 * and then add to the new store.
 *
 * @author dylan
 */
public class MigratingHashStore implements HashStore {

    private final HashStore newHashStore;
    private final HashStore oldHashStore;

    public MigratingHashStore(HashStore newHashStore, HashStore oldHashStore) {
        this.newHashStore = newHashStore;
        this.oldHashStore = oldHashStore;
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        newHashStore.setChunkFanout(hash, blobHashes, actualContentLength);
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        newHashStore.setFileFanout(hash, fanoutHashes, actualContentLength);
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        if (newHashStore.hasFile(fileHash)) {
            return newHashStore.getFileFanout(fileHash);
        } else if (oldHashStore.hasFile(fileHash)) {
            Fanout fanout = oldHashStore.getFileFanout(fileHash);
            if (fanout != null) {
                newHashStore.setFileFanout(fileHash, fanout.getHashes(), fanout.getActualContentLength());
            }
            return fanout;
        }
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        if(newHashStore.hasChunk(fanoutHash)){
            return newHashStore.getChunkFanout(fanoutHash);
        }else if(oldHashStore.hasChunk(fanoutHash)){
            Fanout fanout = oldHashStore.getChunkFanout(fanoutHash);
            if(fanout != null){
                newHashStore.setChunkFanout(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
            }
            return fanout;
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return newHashStore.hasChunk(fanoutHash) || oldHashStore.hasChunk(fanoutHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return newHashStore.hasFile(fileHash) || oldHashStore.hasFile(fileHash);
    }

}
