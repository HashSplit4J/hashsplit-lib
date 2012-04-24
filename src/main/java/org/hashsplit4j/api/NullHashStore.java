package org.hashsplit4j.api;

import java.util.List;

/**
 *
 * @author brad
 */
public class NullHashStore implements HashStore{

    @Override
    public void setFanout(long hash, List<Long> childCrcs, long actualContentLength) {
        
    }

    @Override
    public Fanout getFanout(long fanoutHash) {
        return null;
    }

    @Override
    public boolean hasFanout(long fanoutHash) {
        return false;
    }

}
