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
import org.hashsplit4j.api.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class SimpleFileDbBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDbBlobStore.class);

    static long incrementLong(long val) {
        val = val++;
        if (val > Long.MAX_VALUE - 100) {
            val = 0;
        }
        return val;
    }

    private final BlobStore wrapped;
    private final List<SimpleFileDb> dbs = new ArrayList<>();
    private final Set<String> dbNames = new HashSet<>();
    private final Map<String, SimpleFileDb.DbItem> mapOfItems = new HashMap<>();

    private long hits;
    private long misses;

    public SimpleFileDbBlobStore(BlobStore wrapped) {
        this.wrapped = wrapped;
    }

    public BlobStore getWrapped() {
        return wrapped;
    }

    public String getBlobKey(String hash) {
        return "b-" + hash;
    }

    public void addDb(SimpleFileDb db) {
        dbNames.add(db.getName());
        dbs.add(db);
        mapOfItems.putAll(db.getMapOfItems());
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

    @Override
    public void setBlob(String hash, byte[] bytes) {
        wrapped.setBlob(hash, bytes);
    }

    @Override
    public byte[] getBlob(String hash) {
        String key = getBlobKey(hash);
        SimpleFileDb.DbItem item = mapOfItems.get(key);
        if (item != null) {
            try {
                hits = incrementLong(hits);
                return item.data();
            } catch (IOException ex) {
                log.warn("Exception looking up blob {} from simplefiledb: {}", hash, ex);
            }
        }
        misses = incrementLong(misses);
        return wrapped.getBlob(hash);
    }

    @Override
    public boolean hasBlob(String hash) {
        String key = getBlobKey(hash);
        if (mapOfItems.containsKey(key)) {
            return true;
        }
        return wrapped.hasBlob(hash);
    }

    public long getMisses() {
        return misses;
    }

    public long getHits() {
        return hits;
    }

}
