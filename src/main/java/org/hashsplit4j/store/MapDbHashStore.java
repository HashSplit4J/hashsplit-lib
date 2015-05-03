/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.HashStore;

/**
 *
 * @author dylan
 */
public class MapDbHashStore implements HashStore {

    private final MapDbEnv<String, String> fileDb;
    private final MapDbEnv<String, String> chunkDb;

    public MapDbHashStore(File fileFanoutHome, File chunkFanoutHome) {
        this.fileDb = new MapDbEnv<>(fileFanoutHome);
        this.fileDb.init("fileDb");

        this.chunkDb = new MapDbEnv<>(chunkFanoutHome);
        this.chunkDb.init("chunkDb");
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        String fanout = formatFanout(blobHashes, actualContentLength);
        this.chunkDb.add(hash, fanout);
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        String fanout = formatFanout(fanoutHashes, actualContentLength);
        this.fileDb.add(hash, fanout);
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        String f = this.fileDb.get(fileHash);
        if (f != null) {
            return parseFanout(f);
        }
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        String f = this.chunkDb.get(fanoutHash);
        if (f != null) {
            return parseFanout(f);
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return this.chunkDb.hasHash(fanoutHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return this.fileDb.hasHash(fileHash);
    }

    private String formatFanout(List<String> blobHashes, long actualContentLength) {
        StringBuilder sb = new StringBuilder();

        for (String hash : blobHashes) {
            sb.append(hash).append(",");
        }

        sb.append(actualContentLength);

        return sb.toString();
    }

    private Fanout parseFanout(String fan) {
        String[] parts = fan.split(",");
        List<String> blobHashes = new ArrayList<>();
        Long actualContentLength = null;
        int len = parts.length;
        int count = 0;
        for (String part : parts) {
            if (++count < len) {
                blobHashes.add(part);
            } else {
                actualContentLength = Long.valueOf(part);
            }
        }
        return new FanoutImpl(blobHashes, actualContentLength);
    }
}
