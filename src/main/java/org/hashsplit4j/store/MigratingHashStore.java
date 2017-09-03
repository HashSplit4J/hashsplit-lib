package org.hashsplit4j.store;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.HashFanoutImpl;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.runnables.FanoutQueueRunnable;
import org.slf4j.LoggerFactory;

/**
 * Attempts to get from the new store. If that fails it will get it from the old
 * store, and then add to the new store.
 *
 * @author dylan
 */
public class MigratingHashStore implements HashStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MigratingHashStore.class);

    private final HashStore newHashStore;
    private final HashStore oldHashStore;
    private final ExecutorService exService;
    private final BlockingQueue<HashFanoutImpl> fileQueue = new ArrayBlockingQueue<>(1000);
    private final BlockingQueue<HashFanoutImpl> chunkQueue = new ArrayBlockingQueue<>(1000);

    public MigratingHashStore(HashStore newHashStore, HashStore oldHashStore) {
        this.newHashStore = newHashStore;
        this.oldHashStore = oldHashStore;

        exService = Executors.newCachedThreadPool();
        FanoutQueueRunnable fqr = new FanoutQueueRunnable(this.newHashStore, fileQueue, FanoutQueueRunnable.FanoutType.FILE);
        exService.submit(fqr);

        FanoutQueueRunnable cqr = new FanoutQueueRunnable(this.newHashStore, chunkQueue, FanoutQueueRunnable.FanoutType.CHUNK);
        exService.submit(cqr);
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        try {
            newHashStore.setFileFanout(hash, fanoutHashes, actualContentLength);
        } catch (Exception ex) {
            log.warn("Failed to store file fanout to newHashStore {} with message {}", newHashStore, ex.getMessage(), ex);
            Fanout fanout = new FanoutImpl(fanoutHashes, actualContentLength);
            enqueueFile(hash, fanout);
        }
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        try {
            newHashStore.setChunkFanout(hash, blobHashes, actualContentLength);
        } catch (Exception ex) {
            log.warn("Failed to store chunk fanout to newHashStore {} with message {}", newHashStore, ex.getMessage(), ex);
            Fanout fanout = new FanoutImpl(blobHashes, actualContentLength);
            enqueueChunk(hash, fanout);
        }
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        try {
            if (newHashStore.hasFile(fileHash)) {
                log.info("got file fanout from={}", newHashStore);
                return newHashStore.getFileFanout(fileHash);
            } else {
                log.info("Could not find file fanout {} on newHashStore {}", fileHash, newHashStore);
            }
        } catch (Exception ex) {
            log.warn("getFileFanout Failed on newHashStore {} because of:{}", newHashStore, ex.getMessage(), ex);
        }

        try {
            if (oldHashStore.hasFile(fileHash)) {
                log.info("got file fanout from={}", oldHashStore);
                Fanout fanout = oldHashStore.getFileFanout(fileHash);
                enqueueFile(fileHash, fanout);
                return fanout;
            } else {
                log.info("Could not find file fanout {} on oldHashStore {}", fileHash, oldHashStore);
            }
        } catch (Exception ex) {
            log.warn("getFileFanout Failed on oldHashStore {} because of:{}", oldHashStore, ex.getMessage(), ex);
        }
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        try {
            if (newHashStore.hasChunk(fanoutHash)) {
                log.info("got chunk fanout from={}", newHashStore);
                return newHashStore.getChunkFanout(fanoutHash);
            } else {
                log.info("Could not find chunk fanout {} on newHashStore {}", fanoutHash, newHashStore);
            }
        } catch (Exception ex) {
            log.warn("getChunkFanout Failed on newHashStore {} because of:{}", newHashStore, ex.getMessage(), ex);
        }

        try {
            if (oldHashStore.hasChunk(fanoutHash)) {
                log.info("got chunk fanout from={}", oldHashStore);
                Fanout fanout = oldHashStore.getChunkFanout(fanoutHash);
                enqueueChunk(fanoutHash, fanout);
                return fanout;
            } else {
                log.info("Could not find chunk fanout {} on oldHashStore {}", fanoutHash, oldHashStore);
            }
        } catch (Exception ex) {
            log.warn("getChunkFanout Failed on oldHashStore {} because of:{}", oldHashStore, ex.getMessage(), ex);
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return newHashStore.hasChunk(fanoutHash) || oldHashStore.hasChunk(fanoutHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return newHashStore.hasFile(fileHash) || oldHashStore.hasFile(fileHash);
    }

    private void enqueueFile(String fileHash, Fanout fanout) {
        log.info("Enqueuing file fanout={}", fileHash);
        HashFanoutImpl f = new HashFanoutImpl(fileHash, fanout.getHashes(), fanout.getActualContentLength());
        fileQueue.offer(f);
    }

    private void enqueueChunk(String fanoutHash, Fanout fanout) {
        log.info("Enqueuing chunk fanout={}", fanoutHash);
        HashFanoutImpl f = new HashFanoutImpl(fanoutHash, fanout.getHashes(), fanout.getActualContentLength());
        chunkQueue.offer(f);
    }
}
