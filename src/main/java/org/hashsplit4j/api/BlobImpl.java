/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import java.util.Objects;

/**
 *
 * @author dylan
 */
public class BlobImpl {

    private final String hash;
    private final byte[] bytes;

    public BlobImpl(String hash, byte[] bytes) {
        this.hash = hash;
        this.bytes = bytes;
    }

    public String getHash() {
        return hash;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BlobImpl) {
            BlobImpl b = (BlobImpl) o;
            return b.getHash().equals(hash);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hashC = 7;
        hashC = 71 * hashC + Objects.hashCode(this.hash);
        return hashC;
    }
}
