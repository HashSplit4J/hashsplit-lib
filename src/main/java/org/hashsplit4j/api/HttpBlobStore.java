package org.hashsplit4j.api;

import java.io.IOException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;

/**
 * Implements getting and setting blobs over HTTP
 *
 * @author brad
 */
public class HttpBlobStore implements BlobStore {

    private final HttpClient client;
    private int timeout = 30;
    private String baseUrl;

    public HttpBlobStore(HttpClient client) {
        this.client = client;
    }

    @Override
    public void setBlob(long hash, byte[] bytes) {
        String s = baseUrl + hash;
        PutMethod p = new PutMethod(s);

        HttpMethodParams params = new HttpMethodParams();
        params.setSoTimeout(timeout);
        p.setParams(params);
        try {
            RequestEntity requestEntity = new ByteArrayRequestEntity(bytes);

            p.setRequestEntity(requestEntity);
            int result = client.executeMethod(p);
            if (result < 200 || result >= 300) {
                throw new RuntimeException("Upload failed. result:" + result);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            p.releaseConnection();
        }
    }

    @Override
    public boolean hasBlob(long hash) {
        String s = baseUrl + hash;
        OptionsMethod opts = new OptionsMethod(s);
        int result;
        try {
            result = client.executeMethod(opts);
            if( result >= 500 ) {
                throw new RuntimeException("Server error: " + result);
            }
            if (result == 404) {
                return false;
            }
            if( result >= 200 && result < 300 ) {
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
        GetMethod getMethod = new GetMethod(s);
        int result;
        try {
            result = client.executeMethod(getMethod);
            if (result < 200 || result >= 300) {
                throw new RuntimeException("Upload failed. result:" + result);
            }
            return getMethod.getResponseBody();
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
}
