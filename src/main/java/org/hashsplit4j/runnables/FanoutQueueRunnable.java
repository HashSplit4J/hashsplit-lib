package org.hashsplit4j.runnables;

import java.util.concurrent.BlockingQueue;
import org.hashsplit4j.api.HashFanoutImpl;
import org.hashsplit4j.api.HashStore;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class FanoutQueueRunnable implements Runnable {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(FanoutQueueRunnable.class);

    public static enum FanoutType {

        FILE,
        CHUNK
    }

    private final HashStore hashstore;
    private final BlockingQueue<HashFanoutImpl> queue;
    private final FanoutType fanoutType;

    public FanoutQueueRunnable(HashStore hashStore, BlockingQueue<HashFanoutImpl> queue, FanoutType fanoutType) {
        this.hashstore = hashStore;
        this.queue = queue;
        this.fanoutType = fanoutType;
    }

    @Override
    public void run() {
        HashFanoutImpl fanout = null;
        while (true) {
            try {
                fanout = this.queue.take();
                if (fanout != null) {
                    if (fanoutType.equals(FanoutType.FILE)) {
                        this.hashstore.setFileFanout(fanout.getHash(), fanout.getHashes(), fanout.getActualContentLength());
                    } else if (fanoutType.equals(FanoutType.CHUNK)) {
                        this.hashstore.setChunkFanout(fanout.getHash(), fanout.getHashes(), fanout.getActualContentLength());
                    }
                }
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    log.error("An InterruptedException was thrown with queue {}", queue, ex);
                    throw new RuntimeException(ex);
                } else {
                    log.error("Exception inserting file fanout into store:{} | Msg: {}", hashstore, ex.getMessage(), ex);
                    if (fanout != null) {
                        queue.offer(fanout);
                    }
                }
            }
        }
    }

}
