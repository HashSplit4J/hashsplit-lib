package org.hashsplit4j.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.HashStore;

/**
 * This class is just for debugging and development. It holds all information
 * in memory. It is only suitable for a single parse operation, and is not thread safe
 *
 * @author brad
 */
public class MemoryHashStore implements HashStore{
    
    private Map<String,Fanout> mapOfChunkFanouts = new HashMap<String,Fanout>(); // keyed by the crc of the fanout chunk, gives a list of chunk crc's
    
    private Map<String,Fanout> mapOfFileFanouts = new HashMap<String,Fanout>(); // keyed by the crc of the fanout chunk, gives a list of chunk crc's
    
    @Override
    public void setChunkFanout(String crc, List<String> childCrcs, long actualContentLength) {
        Fanout fanout = new FanoutImpl(childCrcs, actualContentLength);
        mapOfChunkFanouts.put(crc, fanout);
    }

    @Override
    public Fanout getChunkFanout(String fanoutCrc) {
        return mapOfChunkFanouts.get(fanoutCrc);
    }
    
    public long getNumFanouts() {
        return mapOfChunkFanouts.size();
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return getChunkFanout(fanoutHash) != null;
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        Fanout fanout = new FanoutImpl(fanoutHashes, actualContentLength);
        mapOfFileFanouts.put(hash, fanout);

    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        return mapOfFileFanouts.get(fileHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return getFileFanout(fileHash) != null;
    }
}
