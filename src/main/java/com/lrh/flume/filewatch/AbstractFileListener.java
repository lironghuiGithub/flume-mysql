package com.lrh.flume.filewatch;

import com.lrh.flume.datasource.DataSourceReloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractFileListener implements FileListener {
    private static Logger logger = LoggerFactory.getLogger(DataSourceReloader.class);
    private static String[] validFileSuffix = new String[]{".properties", ".json"};
    private static final Object LOCK = new Object();

    @Override
    public void onCreate(Path path) {
        onModify(path);
    }

    @Override
    public void onModify(Path path) {
        if (isValidFile(path)&& Files.exists(path)) {
            synchronized (LOCK) {
                reloadFileUpdate(path);
            }
        }
    }

    @Override
    public void onDelete(Path path) {
        if (isValidFile(path)) {
            synchronized (LOCK) {
                reloadFileDelete(path);
            }
        }
    }

    public void reloadFileAdd(Path path) {
        reloadFileUpdate(path);
    }

    public abstract void reloadFileUpdate(Path path);

    protected void reloadFileDelete(Path path) {

    }

    //验证文件有效性 有时候会出现无效文件 例：D:\LRH\workSpace\Smart-Flume\name2.properties~
    protected boolean isValidFile(Path path) {
        boolean isValid = false;
        for (int i = 0; i < validFileSuffix.length; i++) {
            isValid = path.getFileName().toString().endsWith(validFileSuffix[i]);
            if (isValid) {
                return isValid;
            }
        }
        if (!isValid) {
            logger.error("file is not valid path={},", path.toString());
        }
        return isValid;
    }

    protected String buildSimpleFileName(Path path) {
        return path.getFileName().toString().substring(0, path.getFileName().toString().lastIndexOf("."));
    }
}
