package org.hashsplit4j.store;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author brad
 */
public class SimpleFileDbTest {

    public SimpleFileDbTest() {
    }

    @Test
    public void testSave() throws Exception {
        File keysFile = new File("/tmp/dbkeys");
        File valuesFile = new File("/tmp/dbvals");

        keysFile.delete();
        valuesFile.delete();

        String s1 = "Hello world!!!!!";
        String s2 = "Another string 2";
        String s3 = "Another string ..................... 3";

        SimpleFileDb db = new SimpleFileDb("db1", keysFile, valuesFile);
        byte[] arr1 = s1.getBytes();
        db.put("hello1", arr1);

        byte[] arr2 = db.get("hello1");
        Assert.assertEquals(arr1.length, arr2.length);
        Assert.assertEquals(s1, new String(arr2));

        db.put("helo2", s2.getBytes());
        db.put("hlo3", s3.getBytes());

        SimpleFileDb db2 = new SimpleFileDb("db1", keysFile, valuesFile);
        db2.init();

        Assert.assertEquals(s1, new String(db2.get("hello1")));
        Assert.assertEquals(s2, new String(db2.get("helo2")));
        Assert.assertEquals(s3, new String(db2.get("hlo3")));

        keysFile.delete();
        valuesFile.delete();

    }

}
