package org.hashsplit4j.utils;

import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author dylan
 */
public class FileSystem2UtilsTest {
    
    public FileSystem2UtilsTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of toFileWithPrefix method, of class FileSystem2Utils.
     */
    @Test
    public void testToFileWithPrefix_3args() {
        File root = new File("/home/test/");
        String hash = "36db9daa67c66d0217d8c8511e411dc90b882aff";
        String dirPrefix = "chunks";
        File result = FileSystem2Utils.toFileWithPrefix(root, hash, dirPrefix);
        File expected = new File("/home/test/chunks/36d/b9d/aa6/7c6/6d0/217/d8c/851/1e4/11d/c90/b88/2af/36db9daa67c66d0217d8c8511e411dc90b882aff");
        assertEquals(expected, result);
        
        File result2 = FileSystem2Utils.toFileWithPrefix(root, hash, null);
        File expected2 = new File("/home/test/36d/b9d/aa6/7c6/6d0/217/d8c/851/1e4/11d/c90/b88/2af/36db9daa67c66d0217d8c8511e411dc90b882aff");
        assertEquals(expected2, result2);
    }

    /**
     * Test of toFileWithPrefix method, of class FileSystem2Utils.
     */
    @Test
    public void testToFileWithPrefix_4args() {
        File root = new File("/home/test/");
        String hash = "36db9daa67c66d0217d8c8511e411dc90b882aff";
        String dirPrefix = "chunks";
        File result = FileSystem2Utils.toFileWithPrefix(root, hash, dirPrefix, 6);
        File expected = new File("/home/test/chunks/36db9d/aa67c6/6d0217/d8c851/1e411d/c90b88/36db9daa67c66d0217d8c8511e411dc90b882aff");
        assertEquals(expected, result);
        
        File result2 = FileSystem2Utils.toFileWithPrefix(root, hash, dirPrefix, 4);
        File expected2 = new File("/home/test/chunks/36db/9daa/67c6/6d02/17d8/c851/1e41/1dc9/0b88/36db9daa67c66d0217d8c8511e411dc90b882aff");
        assertEquals(expected2, result2);
        
        File result3 = FileSystem2Utils.toFileWithPrefix(root, hash, null, 8);
        File expected3 = new File("/home/test/36db9daa/67c66d02/17d8c851/1e411dc9/36db9daa67c66d0217d8c8511e411dc90b882aff");
        assertEquals(expected3, result3);
    }

    /**
     * Test of toFile method, of class FileSystem2Utils.
     */
    @Test
    public void testToFile_File_String() {
    }

    /**
     * Test of toFile method, of class FileSystem2Utils.
     */
    @Test
    public void testToFile_3args() {
    }
    
}
