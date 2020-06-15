package com.lrh.flume.sql;

import com.lrh.flume.datasource.DataSourceManager;
import com.lrh.flume.datasource.DataSourceReloader;
import com.lrh.flume.filewatch.FileWatcher;
import com.lrh.flume.sql.model.InsertConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class InsertConfigManager {
    private static Logger logger = LoggerFactory.getLogger(DataSourceManager.class);
    private static final String configFilePath = "sql";
    private static final String filterFileSuffix = ".json";
    private static AtomicBoolean init = new AtomicBoolean();
    /**
     * 启动加载数据源
     */
    public static void start() {
        if (!init.compareAndSet(false, true)) {
            logger.debug("insert config init already");
            return;
        }
        String rootPath = DataSourceManager.class.getClassLoader().getResource("").getPath() + configFilePath;
        File rootFile = new File(rootPath);
        File[] configFileArray = rootFile.listFiles(file -> file.getName().endsWith(filterFileSuffix));
        for (int i = 0; i < configFileArray.length; i++) {
            try {
                InsertConfigReloader.getInstance().reloadFileAdd(Paths.get(configFileArray[i].getAbsolutePath()));
            } catch (Exception e) {
                logger.error("load file error path={}", configFileArray[i].getAbsolutePath());
            }
        }
        /***
         * 注册监听
         */
        FileWatcher.register(rootFile.getAbsolutePath(), InsertConfigReloader.getInstance());
    }

    /**
     * 根据配置文件名称获取配置数据
     *
     * @param configName
     * @return
     */
    public static InsertConfig getInsertEntryConfig(String configName) {
        InsertConfig insertConfig = InsertConfigReloader.getInstance().getInsertEntryConfig(configName);
        if (insertConfig == null) {
            logger.error("InsertEntryConfig not exists error configName= {}", configName);
            throw new IllegalArgumentException("InsertEntryConfig not exists error configName=" + configName);
        }
        return insertConfig;
    }

    public static void main(String[] args) {
        InsertConfigManager.start();

    }
}
