/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * @author dylan
 */
public class FileUtil {

    public static final String DEFAULT_CHARSET = "UTF-8";

    public static String readFile(File file) throws IOException {
        byte[] data = org.apache.commons.io.FileUtils.readFileToByteArray(file);
        return new String(data);
    }

    public static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static String readOrCreateFile(File file, Boolean autoCreate) throws IOException {
        if (file.isFile()) {
            if (!file.exists()) {
                file.createNewFile();
                return null;
            }
            return readFile(file);
        }
        return null;
    }

    public static void writeFile(File file, byte[] data, boolean append) throws IOException {
        org.apache.commons.io.FileUtils.writeByteArrayToFile(file, data, append);
    }

    public static void writeFile(File file, byte[] data, boolean append, Boolean autoCreate) throws IOException {
        if (!file.exists() && autoCreate) {
            File parentFile = file.getAbsoluteFile().getParentFile();
            org.apache.commons.io.FileUtils.forceMkdir(parentFile);
            file.createNewFile();
        }
        writeFile(file, data, append);
    }
}
