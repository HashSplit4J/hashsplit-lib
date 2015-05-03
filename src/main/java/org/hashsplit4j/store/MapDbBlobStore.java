/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.io.File;
import org.hashsplit4j.api.BlobStore;

/**
 *
 * @author dylan
 */
public class MapDbBlobStore implements BlobStore {

    private final MapDbEnv<String, byte[]> blobDb;

    public MapDbBlobStore(File envHome) {
        this.blobDb = new MapDbEnv<>(envHome);
        this.blobDb.init("blobStore");
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        this.blobDb.add(hash, bytes);
    }

    @Override
    public byte[] getBlob(String hash) {
        return this.blobDb.get(hash);
    }

    @Override
    public boolean hasBlob(String hash) {
        return this.blobDb.hasHash(hash);
    }

}
