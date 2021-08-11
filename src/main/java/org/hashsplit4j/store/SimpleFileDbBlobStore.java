/*
 */
package org.hashsplit4j.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hashsplit4j.api.BlobStore;

/**
 *
 * @author brad
 */
public class SimpleFileDbBlobStore implements BlobStore{
    private final List<SimpleFileDb> dbs = new ArrayList<>();
    private final Map<String, SimpleFileDb.DbItem> mapOfItems = new HashMap<>();
    
    public void addDb(SimpleFileDb db) {
        mapOfItems.putAll(db.getMapOfItems());        
    }
    
    public int getNumDbs() {
        return dbs.size();
    }

    public int size() {
        return mapOfItems.size();
    }
    
    @Override
    public void setBlob(String hash, byte[] bytes) {
        throw new UnsupportedOperationException("Not supported. Please add directly to an underlying SimpleFileDb");
    }

    @Override
    public byte[] getBlob(String hash) {
        SimpleFileDb.DbItem item = mapOfItems.get(hash);
        try {
            return item.data();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean hasBlob(String hash) {
        return mapOfItems.containsKey(hash);
    }
    
    
}
