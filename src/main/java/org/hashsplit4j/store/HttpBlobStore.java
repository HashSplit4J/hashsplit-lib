package org.hashsplit4j.store;

import io.milton.common.Path;
import org.hashsplit4j.api.BlobStore;
import org.slf4j.LoggerFactory;

/**
 * Implements getting and setting blobs over HTTP
 *
 * @author brad
 */
public class HttpBlobStore implements BlobStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(HttpBlobStore.class);

    private final HashsplitHttpTransport httpTransport;
    private Path basePath;
    private long gets;
    private long sets;



    public HttpBlobStore(String server, int port, String rootPath, String username, String password) {
        httpTransport = new HashsplitHttpTransport(server, port, username, password);
//        if( !rootPath.startsWith("/")) {
//            throw new IllegalArgumentException("Root path must be absolute,ie start with a slash: " + rootPath);
//        }
        this.basePath = Path.path(rootPath);
    }

    public HttpBlobStore(HashsplitHttpTransport httpTransport, Path rootPath) {
        this.httpTransport = httpTransport;
//        if( rootPath.isRelative() ) {
//            throw new IllegalArgumentException("Root path must be absolute,ie start with a slash: " + rootPath);
//        }
        this.basePath = rootPath;
    }



    @Override
    public void setBlob(String hash, byte[] bytes) {
        if (hasBlob(hash)) {
            return;
        }
        Path destPath = basePath.child(hash + "");
        httpTransport.put(destPath.toString(), bytes);
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] bytes = getBlob(hash);
        return bytes != null;
    }

    @Override
    public byte[] getBlob(String hash) {
        Path destPath = basePath.child(hash);
        return httpTransport.get(destPath.toString());
    }


    /**
     * Base url to PUT to, hash will be appended. Must end with a slash
     *
     * Eg http://myserver/blobs/
     *
     * @return
     */
    public String getBaseUrl() {
        return basePath.toString();
    }

    public void setBaseUrl(String baseUrl) {
        this.basePath = Path.path(baseUrl);
    }

    public long getGets() {
        return gets;
    }

    public long getSets() {
        return sets;
    }

    @Override
    public String toString() {
        return "HttpBlobStore: " + httpTransport.toString();
    }
}
