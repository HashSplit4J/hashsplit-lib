/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.utils.FileUtil;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.utils.FileSystem2Utils;
import org.hashsplit4j.utils.StringFanoutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class FileSystem2HashStore implements HashStore {

    private static final Logger log = LoggerFactory.getLogger(FileSystem2HashStore.class);
    private static final String CHUNK_TYPE = "chunks";
    private static final String FILE_TYPE = "files";
    
    private final File envHome;

    public FileSystem2HashStore(File envHome) {
        this.envHome = envHome;
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        String hashes = StringFanoutUtils.formatFanout(blobHashes, actualContentLength);
        File chunkFanout = FileSystem2Utils.toFileWithPrefix(envHome, hash, CHUNK_TYPE);
        try {
            FileUtil.writeFile(chunkFanout, hashes.getBytes(), false, Boolean.TRUE);
        } catch (IOException ex) {
            log.info("Unable to save chunk fanout to file", ex);
        }
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        String hashes = StringFanoutUtils.formatFanout(fanoutHashes, actualContentLength);
        File fileFanout = FileSystem2Utils.toFileWithPrefix(envHome, hash, FILE_TYPE);
        try {
            FileUtil.writeFile(fileFanout, hashes.getBytes(), false, Boolean.TRUE);
        } catch (IOException ex) {
            log.info("Unable to save file fanout to file", ex);
        }
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        File fileFanout = FileSystem2Utils.toFileWithPrefix(envHome, fileHash, FILE_TYPE);
        if (fileFanout.exists()) {
            try {
                String hashes = FileUtil.readFile(fileFanout);
                return StringFanoutUtils.parseFanout(hashes);
            } catch (IOException ex) {
                log.info("Unable to read file fanout to file", ex);
            }
        }
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        File chunkFanout = FileSystem2Utils.toFileWithPrefix(envHome, fanoutHash, CHUNK_TYPE);
        if (chunkFanout.exists()) {
            try {
                String hashes = FileUtil.readFile(chunkFanout);
                return StringFanoutUtils.parseFanout(hashes);
            } catch (IOException ex) {
                log.info("Unable to read chunk fanout to file", ex);
            }
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        File chunkFanout = FileSystem2Utils.toFileWithPrefix(envHome, fanoutHash, CHUNK_TYPE);
        return chunkFanout.exists();
    }

    @Override
    public boolean hasFile(String fileHash) {
        File fileFanout = FileSystem2Utils.toFileWithPrefix(envHome, fileHash, FILE_TYPE);
        return fileFanout.exists();
    }
}
