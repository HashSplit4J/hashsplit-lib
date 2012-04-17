package org.hashsplit4j;

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

    private Map<Long,Chunk> mapOfChunks = new HashMap<Long, Chunk>();
    private Map<Long,byte[]> mapOfBlobs = new HashMap<Long, byte[]>();
    private Map<Long,List<Long>> mapOfFanouts = new HashMap<Long,List<Long>>(); // keyed by the crc of the fanout chunk, gives a list of chunk crc's

    private long totalSize;
    
    @Override
    public void onChunk(long crc, int start, int finish, byte[] bytes) {
        //System.out.println("Chunk: " + crc + " range:" + start + " - " + finish );
        Chunk chunk = new Chunk();
        chunk.crc = crc;
        chunk.start = start;
        chunk.finish = finish;
        mapOfBlobs.put(crc, bytes);
        mapOfChunks.put(crc, chunk);
        totalSize+=(finish-start);
    }

    @Override
    public void onFanout(long crc, List<Long> childCrcs) {
        //System.out.println("Fanout: " + crc + " child crcs: " + childCrcs.size());
        mapOfFanouts.put(crc, childCrcs);
    }

    @Override
    public List<Long> getCrcsFromFanout(Long fanoutCrc) {
        return mapOfFanouts.get(fanoutCrc);
    }

    @Override
    public byte[] getBlob(Long crc) {
        return mapOfBlobs.get(crc);
    }

    /**
     * A chunk just identifies where the data for a given crc is. Useful on the client side
     */
    public class Chunk {
        long crc;
        int start;
        int finish;
    }

    public long getTotalSize() {
        return totalSize;
    }
    
    public long getNumBlobs() {
        return mapOfBlobs.size();
    }
    
    public long getNumChunks() {
        return mapOfChunks.size();                
    }
    
    public long getNumFanouts() {
        return mapOfFanouts.size();
    }

    public Map<Long, byte[]> getMapOfBlobs() {
        return mapOfBlobs;
    }
    
    
        
}
