/*
 */
package org.hashsplit4j.store;

import java.io.IOException;
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

    protected boolean enableAdd;
    protected SimpleFileDbQueueRunnable queueRunnable;  // if adds are enabled
    protected long maxFileSize = 5 * 1000 * 1000000; // 5GB default
    protected ExecutorService exService;

    private long hits;
    private long misses;
    private long adds;
    private long notFound;
    private long hitDurationMillis;
    private long missDurationMillis;
    private long notFoundDurationMillis;
    private SimpleFileDb addingToDb;

    protected byte[] _get(String key) {
        for (SimpleFileDb db : dbs) {
            byte[] item;
            try {
                item = db.get(key);
                if (item != null) {
                    log.info("_get: key={} data size={}", key, item.length);
                    return item;
                }
            } catch (IOException ex) {
                log.warn("Exception looking for " + key + " in db" + db.getName() + " - {}", ex);
            }
        }
        return null;
    }

    protected boolean _hashKey(String key) {
        for (SimpleFileDb db : dbs) {
            if (db.contains(key)) {
                return true;
            }
        }
        return false;
    }

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

    public Long getValuesFileSize() {
        if (addingToDb != null) {
            return addingToDb.getValuesFileSize();
        }
        return null;
    }

    public String getValuesFilePath() {
        if (addingToDb != null) {
            return addingToDb.getValuesFilePath();
        }
        return null;
    }

    public String getKeysFilePath() {
        if (addingToDb != null) {
            return addingToDb.getKeysFilePath();
        }
        return null;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public synchronized void addDb(SimpleFileDb db) {
        dbNames.add(db.getName());
        dbs.add(db);
        if (enableAdd) {
            if (queueRunnable == null) {
                addingToDb = db;
                queueRunnable = new SimpleFileDbQueueRunnable(db, 1000, maxFileSize);
                exService = Executors.newSingleThreadExecutor();
                exService.submit(this.queueRunnable);
            }
        }
    }

    protected void saveToDb(String key, byte[] bytes) {
        if (queueRunnable != null) {
            adds = incrementLong(adds, 1);
            queueRunnable.add(key, bytes);
        }
    }

    public boolean containsDb(String name) {
        return dbNames.contains(name);
    }

    public int getNumDbs() {
        return dbs.size();
    }

    public int size() {
        int i = 0;
        for (SimpleFileDb db : dbs) {
            i += db.size();
        }
        return i;
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
