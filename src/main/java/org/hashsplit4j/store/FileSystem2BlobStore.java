package org.hashsplit4j.store;

import io.milton.event.EventManager;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import java.io.*;
import org.apache.commons.io.FileUtils;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.utils.FileUtil;
import org.hashsplit4j.event.NewFileBlobEvent;

/**
 * Stores blobs straight into a file system
 *
 * @author brad
 */
public class FileSystem2BlobStore implements BlobStore {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FileSystemBlobStore.class);

    private final File root;
    private final EventManager eventManager;

    public FileSystem2BlobStore(File root) {
        this.root = root;
        this.eventManager = null;
    }

    public FileSystem2BlobStore(File root, EventManager eventManager) {
        this.root = root;
        this.eventManager = eventManager;
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        setBlob(hash, bytes, true);
    }

    public void setBlob(String hash, byte[] bytes, boolean enableEvent) {
        File blob = toPath(hash);
        if (blob.exists()) {
            log.trace("FileSystemBlobStore: setBlob: file exists: {}", blob.getAbsolutePath());
            return; // already exists, so dont overwrite
        }
        try {
            FileUtil.writeFile(blob, bytes, false, true);
        } catch (IOException ex) {
            throw new RuntimeException(blob.getAbsolutePath(), ex);
        }
        log.trace("FileSystemBlobStore: setBlob: wrote file: {} with bytes: {}", blob.getAbsolutePath(), bytes.length);
        if (eventManager != null && enableEvent) {
            try {
                log.info("setBlob: added new blob so tell everyone about it");
                eventManager.fireEvent(new NewFileBlobEvent(hash, blob, root, bytes));
            } catch (ConflictException | BadRequestException | NotAuthorizedException ex) {
                log.error("Exception firing event, but cant do anything about it", ex);
            }
        }
    }

    @Override
    public byte[] getBlob(String hash) {
        File blob = toPath(hash);
        if (!blob.exists()) {
            return null;
        }
        try {
            byte[] arr = FileUtils.readFileToByteArray(blob);
            log.trace("FileSystemBlobStore: getBlob: loaded file: {} for hash: {}", blob.getAbsolutePath(), hash);
            return arr;
        } catch (IOException ex) {
            throw new RuntimeException(blob.getAbsolutePath(), ex);
        }
    }

    @Override
    public boolean hasBlob(String hash) {
        File blob = toPath(hash);
        return blob.exists();
    }

    private File toPath(String hash) {
        String group = hash.substring(0, 3);
        String subGroup = hash.substring(0, 2);
        String pathName = group + "/" + subGroup + "/" + hash;
        File file = new File(root, pathName);
        return file;
    }
}
