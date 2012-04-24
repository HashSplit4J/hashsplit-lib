package org.hashsplit4j.api;

import java.util.List;

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
    public void setFanout(long hash, List<Long> childCrcs, long actualContentLength) {
        firstHashStore.setFanout(hash, childCrcs, actualContentLength);
    }

    @Override
    public Fanout getFanout(long fanoutHash) {
        for (HashStore store  : hashStores) {
            Fanout fanout = store.getFanout(fanoutHash);
            if (fanout != null) {
                return fanout;
            }
        }
        return null;
    }

    @Override
    public boolean hasFanout(long fanoutHash) {
        for (HashStore store  : hashStores) {
            if( store.hasFanout(fanoutHash) ) {
                return true;
            }
        }
        return false;
    }
}
