/*
 * Copyright (C) 2003-2013 eXo Platform SAS.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.hashsplit4j.api;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by The eXo Platform SAS
 * @author <a href="mailto:exo@exoplatform.com">eXoPlatform</a>
 *          
 * @version BerkeleyStoreTest.java Dec 5, 2013
 */
public class BerkeleyStoreTest {

  Charset CHARSET_UTF = Charset.forName("UTF-8");
  
  BerkeleyStore berkeleyStore;
  
  File dbDir;
  
  long cacheSize = 20 * 1024 * 1024;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    dbDir = new File("target/data");
    
    berkeleyStore = new BerkeleyStore(dbDir, cacheSize);
  }

  @Test
  public void testSetBlobWithoutDuplicate() {
    // Insert 10000 entities to berkeley
    for (int j = 10000; j >=0 ;j--) {
      String data = String.valueOf(j);
      
      berkeleyStore.setBlob(DigestUtils.shaHex(data), data.getBytes(CHARSET_UTF));
    }
  }
  
  @Test
  public void testSetBlobWithDuplicate() {
    String key = DigestUtils.shaHex("1");
    String data = "Oracle Berkeley DB Java Edition";
    
    // Try to overwrite the entity {key: "1", value: "Oracle Berkeley DB Java Edition"}
    berkeleyStore.setBlob(key, data.getBytes(CHARSET_UTF));
    
    // The berkeley should keep original data like this entity {key: "1", value: "1"}
    String expResult = new String(berkeleyStore.getBlob(key), CHARSET_UTF);
    
    assertEquals("1", expResult);
  }

  @Test
  public void testGetBlob() {
    String key = DigestUtils.shaHex("10000");
    String expData = new String(berkeleyStore.getBlob(key), CHARSET_UTF);
    
    assertEquals("10000", expData);
  }
  
  /**
   * Test hasBlob when there is a blob
   */
  @Test
  public void testHasBlobWithExist() {
    String key = DigestUtils.shaHex("10");
    
    // Found a Blob
    assertTrue(berkeleyStore.hasBlob(key));
  }
  
  /**
   * Test hasBlob when there is not a blob
   */
  @Test
  public void testHasBlobWithoutExist() {
    String key = DigestUtils.shaHex("20000");
    
    // Not found a Blob
    assertFalse(berkeleyStore.hasBlob(key));
  }
}
