/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;
import org.hashsplit4j.utils.FileUtil;
import org.hashsplit4j.api.HashStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class FileSystemHashStore implements HashStore {

    private static final Logger log = LoggerFactory.getLogger(FileSystemHashStore.class);
    private static final String CHUNK_TYPE = "chunks";
    private static final String FILE_TYPE = "files";
    private static final String FILE_EXT = ".hash";
    
    private final File envHome;

    public FileSystemHashStore(File envHome) {
        this.envHome = envHome;
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        String hashes = formatFanout(blobHashes, actualContentLength);
        File chunkFanout = toPath(CHUNK_TYPE, hash);
        try {
            FileUtil.writeFile(chunkFanout, hashes.getBytes(), false, Boolean.TRUE);
        } catch (IOException ex) {
            log.info("Unable to save chunk fanout to file", ex);
        }
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        String hashes = formatFanout(fanoutHashes, actualContentLength);
        File fileFanout = toPath(FILE_TYPE, hash);
        try {
            FileUtil.writeFile(fileFanout, hashes.getBytes(), false, Boolean.TRUE);
        } catch (IOException ex) {
            log.info("Unable to save file fanout to file", ex);
        }
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        File fileFanout = toPath(FILE_TYPE, fileHash);
        if (fileFanout.exists()) {
            try {
                String hashes = FileUtil.readFile(fileFanout);
                return parseFanout(hashes);
            } catch (IOException ex) {
                log.info("Unable to read file fanout to file", ex);
            }
        }
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        File chunkFanout = toPath(CHUNK_TYPE, fanoutHash);
        if (chunkFanout.exists()) {
            try {
                String hashes = FileUtil.readFile(chunkFanout);
                return parseFanout(hashes);
            } catch (IOException ex) {
                log.info("Unable to read chunk fanout to file", ex);
            }
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        File chunkFanout = toPath(CHUNK_TYPE, fanoutHash);
        return chunkFanout.exists();
    }

    @Override
    public boolean hasFile(String fileHash) {
        File fileFanout = toPath(FILE_TYPE, fileHash);
        return fileFanout.exists();
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

    private File toPath(String type, String hash) {
        String group = hash.substring(0, 3);
        String subGroup = hash.substring(0, 2);
        String pathName = type + "/" + group + "/" + subGroup + "/" + hash + FILE_EXT;
        File file = new File(envHome, pathName);
        return file;
    }
}
