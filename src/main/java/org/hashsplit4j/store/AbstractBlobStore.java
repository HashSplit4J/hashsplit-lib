/*
 */
package org.hashsplit4j.store;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class AbstractBlobStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractBlobStore.class);


    protected long hits;
    protected long misses;
    protected long notFound;
    protected long hitDurationMillis;
    protected long missDurationMillis;
    protected long notFoundDurationMillis;

    public Map<String, Object> getCacheStats() {
        Map<String, Object> map = new HashMap<>();
        map.put("hits", hits);
        if (hits > 0) {
            map.put("hitAvgMs", hitDurationMillis / hits);
        }
        map.put("misses", misses);
        if (misses > 0) {
            map.put("missAvgMs", missDurationMillis / misses);
        }
        return map;
    }

    public long getMisses() {
        return misses;
    }

    public long getHits() {
        return hits;
    }

    protected void recordHit(long startTime) {
        long durationMillis = System.currentTimeMillis() - startTime;
        hits = incrementLong(hits, 1);
        hitDurationMillis = incrementLong(hitDurationMillis, durationMillis);
    }

    protected void recordMiss(long startTime) {
        long durationMillis = System.currentTimeMillis() - startTime;
        misses = incrementLong(misses, 1);
        missDurationMillis = incrementLong(missDurationMillis, durationMillis);
    }

    protected void recordNotFound(long startTime) {
        long durationMillis = System.currentTimeMillis() - startTime;
        notFound = incrementLong(notFound, 1);
        notFoundDurationMillis = incrementLong(notFoundDurationMillis, durationMillis);
    }

    protected long incrementLong(long val, long amount) {
        if (val > Long.MAX_VALUE - 10000) {
            val = 0;
        } else {
            val = val + amount;
        }
        return val;
    }

}
