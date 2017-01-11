package org.hashsplit4j.store;

import io.milton.common.Path;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutSerializationUtils;
import org.hashsplit4j.api.HashStore;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class HttpHashStore implements HashStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(HttpBlobStore.class);

    private final HashsplitHttpTransport httpTransport;
    private final Path fileFanoutPath;
    private final Path chunkFanoutPath;

    public HttpHashStore(HashsplitHttpTransport httpTransport, String fileFanoutPath, String chunkFanoutPath) {
        this.httpTransport = httpTransport;
        this.fileFanoutPath = Path.path(fileFanoutPath);
        this.chunkFanoutPath = Path.path(chunkFanoutPath);
    }

    public HttpHashStore(String server, int port, String username, String password) {
        this.httpTransport = new HashsplitHttpTransport(server, port, username, password);
        this.fileFanoutPath = Path.path("/_hashes/fileFanouts");
        this.chunkFanoutPath = Path.path("/_hashes/chunkFanouts");
    }

    public HttpHashStore(HashsplitHttpTransport httpTransport) {
        this.httpTransport = httpTransport;
        this.fileFanoutPath = Path.path("/_hashes/fileFanouts");
        this.chunkFanoutPath = Path.path("/_hashes/chunkFanouts");
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        throw new UnsupportedOperationException("Cannot write to HTTP hash store");
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        throw new UnsupportedOperationException("Cannot write to HTTP hash store");
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        Path destPath = fileFanoutPath.child(fileHash);
        byte[] arr = httpTransport.get(destPath.toString());
        if (arr == null) {
            log.warn("fanour not found {}", destPath);
            return null;
        }
        ByteArrayInputStream bin = new ByteArrayInputStream(arr);
        Fanout f;
        try {
            f = FanoutSerializationUtils.readFanout(bin);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return f;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        Path destPath = chunkFanoutPath.child(fanoutHash);
        byte[] arr = httpTransport.get(destPath.toString());
        if (arr == null) {
            return null;
        }
        ByteArrayInputStream bin = new ByteArrayInputStream(arr);
        Fanout f;
        try {
            f = FanoutSerializationUtils.readFanout(bin);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return f;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        return getChunkFanout(fanoutHash) != null;
    }

    @Override
    public boolean hasFile(String fileHash) {
        return getFileFanout(fileHash) != null;
    }

    public int getHttpTimeout() {
        return httpTransport.getTimeout();
    }

    public void setHttpTimeout(int timeout) {
        httpTransport.setTimeout(timeout);
    }

}
