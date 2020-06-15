package com.lrh.flume.datasource;

import com.lrh.flume.filewatch.FileWatcher;
import com.lrh.flume.sink.SplitSink;
import com.lrh.flume.sql.InsertConfigManager;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.sql.model.InsertItem;
import com.mysql.cj.xdevapi.PreparableStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class DataSourceManager {
    private static Logger logger = LoggerFactory.getLogger(DataSourceManager.class);
    private static final String configFilePath = "datasource";
    private static final String filterFileSuffix = ".properties";
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
                DataSourceReloader.getInstance().reloadFileAdd(Paths.get(configFileArray[i].getAbsolutePath()));
            } catch (Exception e) {
                logger.error("load file error path={}", configFileArray[i].getAbsolutePath());
            }
        }
        /***
         * 注册监听
         */
        FileWatcher.register(rootFile.getAbsolutePath(), DataSourceReloader.getInstance());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> DataSourceReloader.getInstance().destory()));
    }

    /**
     * 获取链接
     *
     * @param dataSourceName
     * @return
     * @throws SQLException
     */
    public static ConnectionProxy getConnection(String dataSourceName) throws SQLException {
        DataSource dataSource = DataSourceReloader.getInstance().getDataSource(dataSourceName);
        if (dataSource == null) {
            logger.error("dataSource not exists error datasourceName= {}", dataSourceName);
            throw new IllegalArgumentException("dataSource not exists error datasourceName=" + dataSourceName);
        }
        return new ConnectionProxy(dataSource.getConnection());
    }

}


