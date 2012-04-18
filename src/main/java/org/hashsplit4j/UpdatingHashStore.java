package org.hashsplit4j;

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
    public void onFanout(long hash, List<Long> childCrcs) {
        List<Long> fanout = remoteHashStore.getFanout(hash);
        if( fanout == null ) {
            System.out.println("Adding new fanout: " + hash);
            remoteHashStore.onFanout(hash, childCrcs);
        } else {
            System.out.println("Remote has existing fanout: " + hash);
        }
    }

    @Override
    public List<Long> getFanout(Long fanoutHash) {
        return null;
    }

}
