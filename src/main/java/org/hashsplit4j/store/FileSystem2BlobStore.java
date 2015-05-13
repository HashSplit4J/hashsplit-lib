package org.hashsplit4j.store;

import org.hashsplit4j.api.ReceivingBlobStore;
import org.hashsplit4j.api.PushingBlobStore;
import io.milton.event.EventManager;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.utils.FileUtil;
import org.hashsplit4j.event.NewFileBlobEvent;

/**
 * Stores blobs straight into a file system
 *
 * @author brad
 */
public class FileSystem2BlobStore implements BlobStore, PushingBlobStore, ReceivingBlobStore {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FileSystem2BlobStore.class);

    private final File root;
    private final EventManager eventManager;
    private final Queue<BlobImpl> queue = new LinkedList<>();
    private ReceivingBlobStore receivingBlobStore;
    
    private final ScheduledExecutorService processor = Executors
            .newScheduledThreadPool(4);
    
    private Future<?> fsScanner;

    public FileSystem2BlobStore(File root) {
        this.root = root;
        this.eventManager = null;
        processor.submit(new ProcessQueue());
    }

    public FileSystem2BlobStore(File root, EventManager eventManager) throws IOException {
        this.root = root;
        this.eventManager = eventManager;
        processor.submit(new ProcessQueue());
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
        String subGroup = hash.substring(3, 7);
        String pathName = group + "/" + subGroup + "/" + hash;
        File file = new File(root, pathName);
        return file;
    }

    @Override
    public void setReceivingBlobStore(ReceivingBlobStore blobStore) {
        this.receivingBlobStore = blobStore;
        if(fsScanner != null && !fsScanner.isDone()){
            fsScanner.cancel(true);
        }
        fsScanner = processor.submit(new ScanFileSystem(root.toPath()));
    }

    @Override
    public void pushBlobToQueue(String hash, byte[] bytes) {
        BlobImpl blob = new BlobImpl(hash, bytes);
        if (!queue.contains(blob)) {
            queue.offer(new BlobImpl(hash, bytes));

        }
    }

    private class ProcessQueue implements Runnable {

        @Override
        public void run() {
            BlobImpl blob = queue.poll();
            if (blob != null) {
                setBlob(blob.getHash(), blob.getBytes());
            }
            processor.submit(this);
        }
    }

    private void pushBlobTo(File blob) throws IOException {
        if (this.receivingBlobStore != null && blob.isFile() && blob.exists()) {
            String fileName = blob.getName();
            byte[] arr = FileUtils.readFileToByteArray(blob);
            this.receivingBlobStore.pushBlobToQueue(fileName, arr);

        }
    }

    private class ScanFileSystem implements Runnable {

        private final Path startPath;

        public ScanFileSystem(final Path startPath) {
            this.startPath = startPath;
        }

        @Override
        public void run() {
            try {
                Files.walkFileTree(this.startPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Objects.requireNonNull(file);
                        Objects.requireNonNull(attrs);

                        if (!attrs.isDirectory()) {
                            File f = file.toFile();
                            pushBlobTo(f);
                        }

                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException ex) {
                Logger.getLogger(FileSystem2BlobStore.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
}
