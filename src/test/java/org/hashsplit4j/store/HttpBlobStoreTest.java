/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.hashsplit4j.store;

import java.net.URI;
import org.hashsplit4j.api.BlobStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author brad
 */
public class HttpBlobStoreTest {
    
    @Test
    public void testStuff() throws Exception {
        URI uri = new URI("http", null, "10.0.0.1", 80, "/blobs", null, null);
        System.out.println("uri: " + uri);
    }
    
}
