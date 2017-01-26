/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.util.List;
import org.apache.jcs.JCS;
import org.apache.jcs.access.CacheAccess;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.behavior.ICompositeCacheAttributes;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.HashStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class JCSCachingHashStore implements HashStore {

    private static final Logger log = LoggerFactory.getLogger(JCSCachingHashStore.class);

    private final CacheAccess fileCache;
    private final CacheAccess chunkCache;
    private final HashStore hashStore;

    private long fileHits;
    private long fileMisses;
    private long chunkHits;
    private long chunkMisses;

    public JCSCachingHashStore(HashStore hashStore, Integer cacheLimit) throws CacheException {
        this.fileCache = JCS.getInstance("fileHashes");
        ICompositeCacheAttributes fileCca = fileCache.getCacheAttributes();
        if (cacheLimit != null) {
            fileCca.setMaxObjects(cacheLimit);
        }
        fileCca.setUseMemoryShrinker(true);
        this.fileCache.setCacheAttributes(fileCca);

        this.chunkCache = JCS.getInstance("chunkHashes");
        ICompositeCacheAttributes chunkCca = chunkCache.getCacheAttributes();
        if (cacheLimit != null) {
            chunkCca.setMaxObjects(cacheLimit);
        }
        chunkCca.setUseMemoryShrinker(true);
        this.chunkCache.setCacheAttributes(chunkCca);

        if (hashStore == null) {
            this.hashStore = new NullHashStore();
        } else {
            this.hashStore = hashStore;
        }
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        this.hashStore.setChunkFanout(hash, blobHashes, actualContentLength);
        Fanout fanout = new FanoutImpl(blobHashes, actualContentLength);
        try {
            this.chunkCache.put(hash, fanout);
        } catch (CacheException ex) {
            log.warn("Failed to add chunk fanout to cache: " + hash, ex);
        }
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        this.hashStore.setFileFanout(hash, fanoutHashes, actualContentLength);
        Fanout fanout = new FanoutImpl(fanoutHashes, actualContentLength);
        try {
            this.fileCache.put(hash, fanout);
        } catch (CacheException ex) {
            log.warn("Failed to add file fanout to cache: " + hash, ex);
        }
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        Fanout fanout = (Fanout) this.fileCache.get(fileHash);
        if (fanout == null) {
            fanout = this.hashStore.getFileFanout(fileHash);
            if (fanout != null) {
                fileMisses++;
                try {
                    this.fileCache.put(fileHash, fanout);
                } catch (CacheException ex) {
                    log.warn("Failed to add file fanout to cache: " + fileHash, ex);
                }
            }
        } else {
            fileHits++;
        }
        log.info("File Cache miss: hits={} misses={}", fileHits, fileMisses);
        return fanout;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        Fanout fanout = (Fanout) this.chunkCache.get(fanoutHash);
        if (fanout == null) {
            fanout = this.hashStore.getChunkFanout(fanoutHash);
            if (fanout != null) {
                chunkMisses++;
                try {
                    this.chunkCache.put(fanoutHash, fanout);
                } catch (CacheException ex) {
                    log.warn("Failed to add chunk fanout to cache: " + fanoutHash, ex);
                }
            }
        } else {
            chunkHits++;
        }
        log.info("Chunk Cache miss: hits={} misses={}", chunkHits, chunkMisses);
        return fanout;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return this.chunkCache.get(fanoutHash) != null || this.hashStore.hasChunk(fanoutHash);

    }

    @Override
    public boolean hasFile(String fileHash) {
        return this.fileCache.get(fileHash) != null || this.hashStore.hasFile(fileHash);
    }

}
