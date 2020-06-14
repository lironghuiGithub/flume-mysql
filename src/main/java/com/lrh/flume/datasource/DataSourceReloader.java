package com.lrh.flume.datasource;

import com.alibaba.fastjson.JSON;
import com.lrh.flume.filewatch.AbstractFileListener;
import com.lrh.flume.filewatch.FileListener;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class DataSourceReloader extends AbstractFileListener {
    private static Logger logger = LoggerFactory.getLogger(DataSourceReloader.class);
    private static final DataSourceReloader instance = new DataSourceReloader();
    private Map<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();

    private DataSourceReloader() {
    }


    @Override
    public void reloadFileUpdate(Path path) {
        DataSourceConfig dataSourceConfig = buildDataSourceConfig(path);
        DataSource dataSource = buildHikariDataSource(dataSourceConfig);
        DataSource oldDataSource = dataSourceCache.get(dataSourceConfig.getDataSourceName());
        dataSourceCache.put(dataSourceConfig.getDataSourceName(), dataSource);
        closeDatasource(oldDataSource);
        logger.error("realod datasource config file path={}, param={}", path.toString(), JSON.toJSONString(dataSourceConfig));
    }

    @Override
    public void reloadFileDelete(Path path) {
        DataSource oldDataSource = dataSourceCache.remove(buildSimpleFileName(path));
        closeDatasource(oldDataSource);
        logger.error("destory datasource config file path={}", path.toString());
    }

    private DataSourceConfig buildDataSourceConfig(Path path) {
        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        Properties properties = new Properties();
        try {
            properties.load(Files.newBufferedReader(path));
        } catch (IOException e) {
            logger.error("file not found path={}", path.toString(), e);
            return null;
        }
        String driverClassNameString = properties.getProperty("dataSource.driverClassName");
        String jdbcUrlString = properties.getProperty("dataSource.jdbcUrl");
        String userNameString = properties.getProperty("dataSource.userName");
        String passwordString = properties.getProperty("dataSource.password");
        String minimumIdleString = properties.getProperty("dataSource.minimumIdle");
        String maximumPoolSizeString = properties.getProperty("dataSource.maximumPoolSize");
        String idleTimeoutString = properties.getProperty("dataSource.idleTimeout");
        String maxLifetimeString = properties.getProperty("dataSource.maxLifetime");

        dataSourceConfig.setDataSourceName(buildSimpleFileName(path));
        dataSourceConfig.setDriverClassName(driverClassNameString);
        dataSourceConfig.setJdbcUrl(jdbcUrlString);
        dataSourceConfig.setUserName(userNameString);
        dataSourceConfig.setPassword(passwordString);

        if (minimumIdleString != null) {
            dataSourceConfig.setMinimumIdle(Integer.parseInt(minimumIdleString.trim()));
        }
        if (maximumPoolSizeString != null) {
            dataSourceConfig.setMaximumPoolSize(Integer.parseInt(maximumPoolSizeString.trim()));
        }
        if (idleTimeoutString != null) {
            dataSourceConfig.setIdleTimeout(Long.parseLong(idleTimeoutString.trim()));
        }
        if (maxLifetimeString != null) {
            dataSourceConfig.setMaxLifetime(Long.parseLong(maxLifetimeString.trim()));
        }
        return dataSourceConfig;
    }

    private DataSource buildHikariDataSource(DataSourceConfig dataSourceConfig) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(dataSourceConfig.getDriverClassName());
        hikariConfig.setJdbcUrl(dataSourceConfig.getJdbcUrl());
        hikariConfig.setUsername(dataSourceConfig.getUserName());
        hikariConfig.setPassword(dataSourceConfig.getPassword());
        hikariConfig.setMinimumIdle(dataSourceConfig.getMinimumIdle());
        hikariConfig.setMaximumPoolSize(dataSourceConfig.getMaximumPoolSize());
        hikariConfig.setMaxLifetime(dataSourceConfig.getMaxLifetime());
        hikariConfig.setIdleTimeout(dataSourceConfig.getIdleTimeout());
        return new HikariDataSource(hikariConfig);
    }

    public void destory() {
        if (dataSourceCache != null && dataSourceCache.size() > 0) {
            for (DataSource dataSource : dataSourceCache.values()) {
                closeDatasource(dataSource);
            }
        }
    }

    public void closeDatasource(DataSource dataSource) {
        if (dataSource != null) {
            HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
            hikariDataSource.close();
        }
    }

    public static DataSourceReloader getInstance() {
        return instance;
    }

    private Map<String, DataSource> getDataSourceCache() {
        return dataSourceCache;
    }

    public DataSource getDataSource(String dataSourceName) {
        return getInstance().getDataSourceCache().get(dataSourceName);
    }
}
