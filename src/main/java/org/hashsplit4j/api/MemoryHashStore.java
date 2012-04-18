package org.hashsplit4j.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is just for debugging and development. It holds all information
 * in memory. It is only suitable for a single parse operation, and is not thread safe
 *
 * @author brad
 */
public class MemoryHashStore implements HashStore{
    
    private Map<Long,List<Long>> mapOfFanouts = new HashMap<Long,List<Long>>(); // keyed by the crc of the fanout chunk, gives a list of chunk crc's
    
    
    @Override
    public void setFanout(long crc, List<Long> childCrcs) {
        //System.out.println("Fanout: " + crc + " child crcs: " + childCrcs.size());
        mapOfFanouts.put(crc, childCrcs);
    }

    @Override
    public List<Long> getFanout(long fanoutCrc) {
        return mapOfFanouts.get(fanoutCrc);
    }
    
    public long getNumFanouts() {
        return mapOfFanouts.size();
    }

    @Override
    public boolean hasFanout(long fanoutHash) {
        return getFanout(fanoutHash) != null;
    }
}
