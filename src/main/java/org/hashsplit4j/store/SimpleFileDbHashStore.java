/*
 */
package org.hashsplit4j.store;

import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.utils.StringFanoutUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class SimpleFileDbHashStore extends AbstractFileDbBlobStore implements HashStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDbHashStore.class);

    private final HashStore wrapped;

    public SimpleFileDbHashStore(HashStore wrapped) {
        this.wrapped = wrapped;
    }

    public HashStore getWrapped() {
        return wrapped;
    }

    public String getChunkKey(String hash) {
        return "c-" + hash;
    }

    public String getFileKey(String hash) {
        return "f-" + hash;
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        wrapped.setChunkFanout(hash, blobHashes, actualContentLength);
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        wrapped.setFileFanout(hash, fanoutHashes, actualContentLength);
    }

    private Fanout toFanout(byte[] arr) {
        if (arr == null) {
            log.info("toFanout: item data is null");
            return null;
        }

        String s = new String(arr);
        Fanout f = StringFanoutUtils.parseFanout(s);
        //log.info("toFanout: persisted={} contentlength={} firsthash={}", s, f.getActualContentLength(), f.getHashes().get(0));
        return f;
    }

    @Override
    public Fanout getFileFanout(String hash) {
        long startTime = System.currentTimeMillis();
        String key = getFileKey(hash);
        byte[] data = _get(key);
        if (data != null) {
            recordHit(startTime);
            return toFanout(data);
        }
        startTime = System.currentTimeMillis();
        try {
            Fanout f = wrapped.getFileFanout(hash);
            if (f != null) {
                if (enableAdd) {
                    String s = StringFanoutUtils.formatFanout(f.getHashes(), f.getActualContentLength());
                    saveToDb(key, s.getBytes());
                }
            }
            return f;
        } finally {
            recordMiss(startTime);
        }
    }

    @Override
    public Fanout getChunkFanout(String hash) {
        //log.info("getChunkFanout: hash={}", hash);
        long startTime = System.currentTimeMillis();
        String key = getChunkKey(hash);
        byte[] data = _get(key);
        if (data != null) {
            recordHit(startTime);
            return toFanout(data);
        }
        startTime = System.currentTimeMillis();
        try {
            Fanout f = wrapped.getChunkFanout(hash);
            if (f != null) {
                if (enableAdd) {
                    String s = StringFanoutUtils.formatFanout(f.getHashes(), f.getActualContentLength());
                    saveToDb(key, s.getBytes());
                }
                //log.info("getChunkFanout: hash={} contentlength={} hashes={}", hash, f.getActualContentLength(), f.getHashes());
            } else {
                //log.info("getChunkFanout: not found hash={} from wrapped={}", hash, wrapped);
            }
            return f;
        } finally {
            recordMiss(startTime);
        }
    }

    @Override
    public boolean hasChunk(String hash) {
        String key = getChunkKey(hash);
        if (_hashKey(key) ) {
            return true;
        }
        return wrapped.hasChunk(hash);
    }

    @Override
    public boolean hasFile(String hash) {
        String key = getFileKey(hash);
        if (_hashKey(key) ) {
            return true;
        }
        return wrapped.hasFile(hash);
    }

}
