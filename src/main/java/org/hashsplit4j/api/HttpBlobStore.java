package org.hashsplit4j.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;

/**
 * Implements getting and setting blobs over HTTP
 *
 * @author brad
 */
public class HttpBlobStore implements BlobStore {

    private final HttpClient client;
    private final HashCache hashCache;
    private int timeout = 30000;
    private String baseUrl;
    private long gets;
    private long sets;

    public HttpBlobStore(HttpClient client, HashCache hashCache) {
        this.client = client;
        this.hashCache = hashCache;
    }

    @Override
    public void setBlob(long hash, byte[] bytes) {
        if (hasBlob(hash)) {
            return;
        }
        String s = baseUrl + hash;
        HttpPut p = new HttpPut(s);

        try {
            HttpEntity requestEntity = new ByteArrayEntity(bytes);

            p.setEntity(requestEntity);
            HttpResponse resp = client.execute(p);
            int result = resp.getStatusLine().getStatusCode();
            if (result < 200 || result >= 300) {
                throw new RuntimeException("Upload failed. result:" + result);
            }
            if (hashCache != null) {
                hashCache.setHash(hash);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean hasBlob(long hash) {
        if (hashCache != null) {
            if (hashCache.hasHash(hash)) { // say that 3 times quickly!!!  :)
                return true;
            }
        }
        String s = baseUrl + hash;
        HttpOptions opts = new HttpOptions(s);
        int result;
        try {
            HttpResponse resp = client.execute(opts);
            result = resp.getStatusLine().getStatusCode();
            if (result >= 500) {
                throw new RuntimeException("Server error: " + result);
            }
            if (result == 404) {
                return false;
            }
            if (result >= 200 && result < 300) {
                if (hashCache != null) {
                    hashCache.setHash(hash);
                }
                return true;
            }
            throw new RuntimeException("Invalid response code: " + result);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public byte[] getBlob(long hash) {
        String s = baseUrl + hash;
        HttpGet getMethod = new HttpGet(s);
        int result;
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            result = HttpUtils.executeHttpWithStatus(client, getMethod, bout);
            if (result < 200 || result >= 300) {
                throw new RuntimeException("Upload failed. result:" + result);
            }
            if (hashCache != null) {
                hashCache.setHash(hash);
            }
            return bout.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Base url to PUT to, hash will be appended. Must end with a slash
     *
     * Eg http://myserver/blobs
     *
     * @return
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public long getGets() {
        return gets;
    }

    public long getSets() {
        return sets;
    }   
}
