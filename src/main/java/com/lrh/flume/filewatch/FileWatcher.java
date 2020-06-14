package com.lrh.flume.filewatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileWatcher {
    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);
    private static WatchService watchService;
    private static ExecutorService executorService;
    private static Map<Path, FileListener> reloadCache;

    static {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            executorService = Executors.newSingleThreadExecutor();
            reloadCache = new ConcurrentHashMap<>();
            executorService.execute(new watchTask());
        } catch (IOException e) {
            logger.error("init FileWatcher", e);
        }

    }

    public static void register(String filePath, FileListener fileReload) {
        Path path = Paths.get(filePath);
        register(path, fileReload);

    }

    public static void register(Path path, FileListener fileReload) {
        Path registerPath = null;
        if (Files.isDirectory(path)) {
            registerPath = path;
        } else {
            registerPath = path.getParent();
        }
        try {
            registerPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_CREATE);
            reloadCache.putIfAbsent(registerPath, fileReload);
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public static void fireEvent(Path registerPath, FileListener listener, WatchEvent event) {
        Path path = (Path) event.context();
        Path absolutePath = Paths.get(registerPath.toString(), path.toString());
        logger.info("······file chanages event type={},path={}", event.kind(), absolutePath.toString());
        if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
            listener.onCreate(absolutePath);
        } else if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
            listener.onModify(absolutePath);
        } else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
            listener.onDelete(absolutePath);
        }

    }

    static class watchTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    WatchKey key = null;
                    key = watchService.take();
                    Path registerPath = (Path) key.watchable();
                    FileListener fileListener = reloadCache.get(registerPath);
                    for (WatchEvent event : key.pollEvents()) {
                        try {
                            fireEvent(registerPath, fileListener, event);
                        } catch (Exception e) {
                            logger.error("", e);
                        }
                    }
                    key.reset();
                } catch (Exception e) {
                    logger.error("", e);
                }

            }
        }
    }
}
