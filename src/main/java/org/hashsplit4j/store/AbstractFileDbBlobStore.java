/*
 */
package org.hashsplit4j.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hashsplit4j.runnables.SimpleFileDbQueueRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class AbstractFileDbBlobStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractFileDbBlobStore.class);

    protected final List<SimpleFileDb> dbs = new ArrayList<>();
    protected final Set<String> dbNames = new HashSet<>();
    protected final Map<String, SimpleFileDb.DbItem> mapOfItems = new HashMap<>();

    protected boolean enableAdd;
    protected SimpleFileDbQueueRunnable queueRunnable;  // if adds are enabled
    protected long maxFileSize = 5 * 1000 * 1000000; // 5GB default
    protected ExecutorService exService;

    private long hits;
    private long misses;
    private long adds;
    private long hitDurationMillis;
    private long missDurationMillis;

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
        map.put("adds", adds);
        return map;
    }

    public boolean isEnableAdd() {
        return enableAdd;
    }

    public void setEnableAdd(boolean enableAdd) {
        this.enableAdd = enableAdd;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public void addDb(SimpleFileDb db) {
        dbNames.add(db.getName());
        dbs.add(db);
        mapOfItems.putAll(db.getMapOfItems());
        if (enableAdd) {
            if (queueRunnable == null) {
                queueRunnable = new SimpleFileDbQueueRunnable(db, 1000, maxFileSize);
                exService = Executors.newSingleThreadExecutor();
                exService.submit(this.queueRunnable);
            }
        }
    }

    protected void saveToDb(String hash, byte[] bytes) {
        if (queueRunnable != null) {
            adds = incrementLong(adds, 1);
            queueRunnable.add(hash, bytes);
        }
    }

    public boolean containsDb(String name) {
        return dbNames.contains(name);
    }

    public int getNumDbs() {
        return dbs.size();
    }

    public int size() {
        return mapOfItems.size();
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
        long durationMillis = System.currentTimeMillis()- startTime;
        misses = incrementLong(misses, 1);
        missDurationMillis = incrementLong(missDurationMillis, durationMillis);
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
