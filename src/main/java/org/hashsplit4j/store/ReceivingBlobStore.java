/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import org.hashsplit4j.api.BlobStore;

/**
 *
 * @author dylan
 */
public interface ReceivingBlobStore extends BlobStore {

    public void pushBlob(String hash, byte[] bytes);
}
