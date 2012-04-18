package org.hashsplit4j;

import java.util.List;

/**
 *
 * @author brad
 */
public class MultipleHashStore implements HashStore{
    
    private final HashStore hashStore1;
    private final HashStore hashStore2;

    private int store1Hits;
    private int store2Hits;    
    
    public MultipleHashStore(HashStore hashStore1, HashStore hashStore2) {
        this.hashStore1 = hashStore1;
        this.hashStore2 = hashStore2;
    }
    
   

    @Override
    public void setFanout(long hash, List<Long> childCrcs) {
        hashStore1.setFanout(hash, childCrcs);
    }

    @Override
    public List<Long> getFanout(long fanoutHash) {
        List<Long> list = hashStore1.getFanout(fanoutHash);
        if( list == null ) {
            list = hashStore2.getFanout(fanoutHash);            
            if( list != null ) {
                store2Hits++;
            }
        } else {
            store1Hits++;
        }
        return list;
    }


    public int getStore1Hits() {
        return store1Hits;
    }

    public int getStore2Hits() {
        return store2Hits;
    }

    @Override
    public boolean hasFanout(long fanoutHash) {
        return getFanout(fanoutHash) != null;
    }
}
