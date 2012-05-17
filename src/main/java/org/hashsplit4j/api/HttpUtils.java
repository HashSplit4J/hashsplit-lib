package org.hashsplit4j.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;

/**
 *
 * @author brad
 */
public class HttpUtils {
    public static int executeHttpWithStatus(HttpClient client, HttpUriRequest m, OutputStream out) throws IOException {
        HttpResponse resp = client.execute(m);
        HttpEntity entity = resp.getEntity();
        if( entity != null ) {
            InputStream in = null;
            try {
                in = entity.getContent();
                if( out != null ) {
                    IOUtils.copy(in, out);
                }
            } finally {
                IOUtils.closeQuietly(in);
            }
        }
        return resp.getStatusLine().getStatusCode();
    }        
}
