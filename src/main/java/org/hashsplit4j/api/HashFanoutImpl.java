/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import java.util.List;

/**
 *
 * @author dylan
 */
public class HashFanoutImpl implements Fanout {

    private String hash;
    private List<String> hashes;
    private long actualContentLength;

    public HashFanoutImpl() {
    }

    public HashFanoutImpl(String hash, List<String> hashes, long actualContentLength) {
        this.hash = hash;
        this.hashes = hashes;
        this.actualContentLength = actualContentLength;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    @Override
    public long getActualContentLength() {
        return actualContentLength;
    }

    public void setActualContentLength(long actualContentLength) {
        this.actualContentLength = actualContentLength;
    }

    @Override
    public List<String> getHashes() {
        return hashes;
    }

    public void setHashes(List<String> hashes) {
        this.hashes = hashes;
    }
}
