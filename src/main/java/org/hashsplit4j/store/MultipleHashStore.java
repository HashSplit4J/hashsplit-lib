package org.hashsplit4j.store;

import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;

/**
 * Normally used for optimising network traffic. For example, if you have a
 * local hash store with a subset of data, and a remote hash store with a full
 * set, you would use this with both, with the local hash store given first
 *
 * Mutating operations are only delegated to the first store
 *
 * @author brad
 */
public class MultipleHashStore implements HashStore {

    private final List<? extends HashStore> hashStores;
    private final HashStore firstHashStore;

    public MultipleHashStore(List<? extends HashStore> hashStores) {
        this.hashStores = hashStores;
        firstHashStore = hashStores.get(0);
    }

    @Override
    public void setChunkFanout(String hash, List<String> childCrcs, long actualContentLength) {
        firstHashStore.setChunkFanout(hash, childCrcs, actualContentLength);
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        for (HashStore store  : hashStores) {
            Fanout fanout = store.getChunkFanout(fanoutHash);
            if (fanout != null) {
                return fanout;
            }
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        for (HashStore store  : hashStores) {
            if( store.hasChunk(fanoutHash) ) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        firstHashStore.setChunkFanout(hash, fanoutHashes, actualContentLength);
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        for (HashStore store  : hashStores) {
            Fanout fanout = store.getFileFanout(fileHash);
            if (fanout != null) {
                return fanout;
            }
        }
        return null;
    }

    @Override
    public boolean hasFile(String fileHash) {
        for (HashStore store  : hashStores) {
            if( store.hasChunk(fileHash) ) {
                return true;
            }
        }
        return false;
    }
}
