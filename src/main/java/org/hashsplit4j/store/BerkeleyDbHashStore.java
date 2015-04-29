/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.io.File;
import java.util.List;
import org.hashsplit4j.api.BerkeleyDbEnv;
import org.hashsplit4j.api.BerkeleyHashDbAccessor;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.api.Hash;
import org.hashsplit4j.api.HashStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class BerkeleyDbHashStore implements HashStore {

    private final Logger log = LoggerFactory.getLogger(BerkeleyDbHashStore.class);

    private final int nPrefGroup;
    private final int nPrefSubGroup;

    /**
     * Encapsulates the environment and data store
     */
    private final BerkeleyDbEnv dbFileEnv = new BerkeleyDbEnv();
    private final BerkeleyDbEnv dbChunkEnv = new BerkeleyDbEnv();

    private final BerkeleyHashDbAccessor fileAccessor;
    private final BerkeleyHashDbAccessor chunkAccessor;

    public BerkeleyDbHashStore(File fileEnvHome, File chunkEnvHome, int nPrefGroup, int nPrefSubGroup) {
        this.nPrefGroup = nPrefGroup;
        this.nPrefSubGroup = nPrefSubGroup;

        dbFileEnv.openEnv(fileEnvHome, // path to the environment home
                false);         // Environment read-only?

        dbChunkEnv.openEnv(chunkEnvHome, // path to the environment home
                false);         // Environment read-only?

        fileAccessor = new BerkeleyHashDbAccessor(dbFileEnv.getEntityStore());
        chunkAccessor = new BerkeleyHashDbAccessor(dbChunkEnv.getEntityStore());
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        if (hash == null || blobHashes == null) {
            throw new RuntimeException("hash and blobHashes can not be null for store chunk fanout function");
        }

        String group = hash.substring(0, nPrefGroup);
        String subGroup = hash.substring(0, nPrefSubGroup);

        chunkAccessor.addToHashByIndex(new Hash(hash, group, subGroup, blobHashes, actualContentLength));
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        if (hash == null || fanoutHashes == null) {
            throw new RuntimeException("hash and fanoutHashes can not be null for store file fanout function");
        }

        String group = hash.substring(0, nPrefGroup);
        String subGroup = hash.substring(0, nPrefSubGroup);

        fileAccessor.addToHashByIndex(new Hash(hash, group, subGroup, fanoutHashes, actualContentLength));
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        Hash dbHash = fileAccessor.getFromHashByIndex(fileHash);
        return parseHash(dbHash);
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
         Hash dbHash = chunkAccessor.getFromHashByIndex(fanoutHash);
         return parseHash(dbHash);
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return chunkAccessor.containsHashByIndex(fanoutHash);
    }

    @Override
    public boolean hasFile(String fileHash) {
        return fileAccessor.containsHashByIndex(fileHash);
    }

    private Fanout parseHash(Hash dbHash) {
        Fanout fanout = null;
        if (dbHash != null) {
            fanout = new FanoutImpl(dbHash.getHashes(), dbHash.getActualContentLength());
        }
        return fanout;
    }

}
