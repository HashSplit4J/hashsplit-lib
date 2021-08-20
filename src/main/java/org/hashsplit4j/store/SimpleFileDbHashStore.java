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
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import static org.hashsplit4j.store.SimpleFileDbBlobStore.incrementLong;
import org.hashsplit4j.utils.StringFanoutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class SimpleFileDbHashStore implements HashStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDbHashStore.class);

    private final HashStore wrapped;
    private final List<SimpleFileDb> dbs = new ArrayList<>();
    private final Set<String> dbNames = new HashSet<>();
    private final Map<String, SimpleFileDb.DbItem> mapOfItems = new HashMap<>();

    private long hits;
    private long misses;

    public SimpleFileDbHashStore(HashStore wrapped) {
        this.wrapped = wrapped;
    }

    public HashStore getWrapped() {
        return wrapped;
    }



    public String getChunkKey(String hash) {
        return "c-" + hash;
    }

    public String getFileKey(String hash) {
        return "f-" + hash;
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
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        wrapped.setChunkFanout(hash, blobHashes, actualContentLength);
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        wrapped.setFileFanout(hash, fanoutHashes, actualContentLength);
    }

    private Fanout toFanout(SimpleFileDb.DbItem item) throws IOException {
        byte[] arr = item.data();
        if (arr == null) {
            return null;
        }
        String s = new String(arr);
        return StringFanoutUtils.parseFanout(s);
    }

    @Override
    public Fanout getFileFanout(String hash) {
        String key = getFileKey(hash);
        SimpleFileDb.DbItem item = mapOfItems.get(key);
        if (item != null) {
            try {
                hits = incrementLong(hits);
                return toFanout(item);
            } catch (IOException ex) {
                log.warn("Exception looking up file {} from simplefiledb: {}", hash, ex);
            }
        }
        misses = incrementLong(misses);
        return wrapped.getFileFanout(hash);
    }

    @Override
    public Fanout getChunkFanout(String hash) {
        String key = getChunkKey(hash);
        SimpleFileDb.DbItem item = mapOfItems.get(key);
        if (item != null) {
            try {
                hits = incrementLong(hits);
                return toFanout(item);
            } catch (IOException ex) {
                log.warn("Exception looking up chunk {} from simplefiledb: {}", hash, ex);
            }
        }
        misses = incrementLong(misses);
        return wrapped.getChunkFanout(hash);
    }

    @Override
    public boolean hasChunk(String hash) {
        String key = getChunkKey(hash);
        if (mapOfItems.containsKey(key)) {
            return true;
        }
        return wrapped.hasChunk(hash);
    }

    @Override
    public boolean hasFile(String hash) {
        String key = getFileKey(hash);
        if (mapOfItems.containsKey(key)) {
            return true;
        }
        return wrapped.hasFile(hash);
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }


}
