package org.hashsplit4j.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.HashStore;

/**
 *
 * @author brad
 */
public class CachingHashStore implements HashStore {

    private final HashStore hashStore;
    private final ConcurrentLinkedHashMap<String, FanoutImpl> chunkCache;
    private final ConcurrentLinkedHashMap<String, FanoutImpl> fileCache;

    public CachingHashStore(HashStore hashStore, int capacity) {
        this.hashStore = hashStore;
        chunkCache = new ConcurrentLinkedHashMap.Builder()
                .maximumWeightedCapacity(capacity)
                .build();
        fileCache = new ConcurrentLinkedHashMap.Builder()
                .maximumWeightedCapacity(capacity)
                .build();
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        hashStore.setChunkFanout(hash, blobHashes, actualContentLength);
        FanoutImpl i = new FanoutImpl(blobHashes, actualContentLength);
        chunkCache.putIfAbsent(hash, i);
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {

        Fanout f = chunkCache.get(fanoutHash);
        if (f == null) {
            f = hashStore.getChunkFanout(fanoutHash);
            if (f == null) {
                return f;
            }
        }
        FanoutImpl i = new FanoutImpl(f.getHashes(), f.getActualContentLength());
        chunkCache.putIfAbsent(fanoutHash, i);
        return i;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        Fanout f = chunkCache.get(fanoutHash);
        if (f != null) {
            return true;
        }
        return hashStore.hasChunk(fanoutHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        Fanout f = fileCache.get(fileHash);
        if (f != null) {
            return true;
        }
        return hashStore.hasChunk(fileHash);
    }

    @Override
    public void setFileFanout(String fileHash, List<String> fanoutHashes, long actualContentLength) {
        hashStore.setFileFanout(fileHash, fanoutHashes, actualContentLength);
        FanoutImpl i = new FanoutImpl(fanoutHashes, actualContentLength);
        fileCache.putIfAbsent(fileHash, i);
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        Fanout f = fileCache.get(fileHash);
        if (f == null) {
            f = hashStore.getFileFanout(fileHash);
            if (f == null) {
                return f;
            }
        }
        FanoutImpl i = new FanoutImpl(f.getHashes(), f.getActualContentLength());
        fileCache.putIfAbsent(fileHash, i);
        return i;
    }

    @Override
    public String toString() {
        return "CachingHashStore(" + this.hashStore + ")";
    }


}
