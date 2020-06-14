package com.lrh.flume.filewatch;

import java.nio.file.Path;

public interface FileListener {
    /**
     * 在监视目录中新增文件时的处理操作
     * @param path
     */
    void onCreate(Path path);

    /**
     * 在监视目录中修改文件时的处理操作
     * @param path
     */
    void onModify(Path path);

    /**
     * 在监视目录中删除文件时的处理操作
     * @param path
     */
    void onDelete(Path path);

}
