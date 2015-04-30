/*
 * Copyright 2012 McEvoy Software Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hashsplit4j.store;

import org.apache.jcs.JCS;
import org.apache.jcs.access.CacheAccess;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.behavior.ICompositeCacheAttributes;
import org.hashsplit4j.api.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BlobStore which uses a JCS cache to store blobs in memory and disk
 *
 * @author brad
 */
public class JCSCachingBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(JCSCachingBlobStore.class);

    private final CacheAccess cache;

    private final BlobStore blobStore;

    private long hits;
    private long misses;

    public JCSCachingBlobStore(BlobStore blobStore, Integer capacity) throws CacheException {
        this.blobStore = blobStore;
        cache = JCS.getInstance("blobs");
        ICompositeCacheAttributes cacheCca = cache.getCacheAttributes();
        if (capacity != null) {
            cacheCca.setMaxObjects(capacity);
        }
        cacheCca.setUseMemoryShrinker(true);
        this.cache.setCacheAttributes(cacheCca);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        blobStore.setBlob(hash, bytes);
        try {
            if (cache.get(hash) == null) {
                cache.putSafe(hash, bytes);
            }
        } catch (CacheException ex) {
            log.warn("Failed to add blob to cache: " + hash, ex);
        }
    }

    @Override
    public byte[] getBlob(String hash) {
        byte[] arr = (byte[]) cache.get(hash);
        if (arr == null) {
            arr = blobStore.getBlob(hash);
            if (arr != null) {
                log.info("JCSCachingBlobStore cache miss: hits={} misses={}", hits, misses);
                misses++;
                try {
                    if (cache.get(hash) == null) {
                        cache.putSafe(hash, arr);
                    }
                } catch (CacheException ex) {
                    log.warn("Failed to add blob to cache: " + hash, ex);
                }
            }
        } else {
            hits++;
        }
        return arr;
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] arr = getBlob(hash);
        return arr != null;
    }
}
