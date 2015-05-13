/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import org.hashsplit4j.api.ReceivingBlobStore;
import org.hashsplit4j.api.PushingBlobStore;
import org.hashsplit4j.api.BlobStore;

/**
 *
 * @author dylan
 */
public class MigratingBlobStore implements BlobStore{
    
    private final BlobStore newBlobStore;
    private final BlobStore oldBlobStore;
    
    public MigratingBlobStore(BlobStore newBlobStore, BlobStore oldBlobStore){
        this.newBlobStore = newBlobStore;
        this.oldBlobStore = oldBlobStore;
        if(oldBlobStore instanceof PushingBlobStore){
            PushingBlobStore pBlobStore = (PushingBlobStore) oldBlobStore;
            if(newBlobStore instanceof ReceivingBlobStore){
                ReceivingBlobStore rBlobStore = (ReceivingBlobStore) newBlobStore;
                pBlobStore.setReceivingBlobStore(rBlobStore);
            }
        }
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        newBlobStore.setBlob(hash, bytes);
    }

    @Override
    public byte[] getBlob(String hash) {
        if(newBlobStore.hasBlob(hash)){
            return newBlobStore.getBlob(hash);
        }else if(oldBlobStore.hasBlob(hash)){
            byte [] data = oldBlobStore.getBlob(hash);
            newBlobStore.setBlob(hash, data);
            return data;
        }
        return null;
    }

    @Override
    public boolean hasBlob(String hash) {
        return newBlobStore.hasBlob(hash) || oldBlobStore.hasBlob(hash);
    }
    
}
