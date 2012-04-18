package org.hashsplit4j.api;

import java.util.List;

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
    public void setFanout(long hash, List<Long> childCrcs) {
        if( !remoteHashStore.hasFanout(hash) ) {
            System.out.println("Adding new fanout: " + hash);
            remoteHashStore.setFanout(hash, childCrcs);
        } else {
            System.out.println("Remote has existing fanout: " + hash);
        }
    }

    @Override
    public List<Long> getFanout(long fanoutHash) {
        return null;
    }

    @Override
    public boolean hasFanout(long fanoutHash) {
        return false;
    }

}
