package com.lrh.flume.sql;

import com.alibaba.fastjson.JSONObject;
import com.lrh.flume.filewatch.AbstractFileListener;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InsertConfigReloader extends AbstractFileListener {
    private static Logger logger = LoggerFactory.getLogger(InsertConfigReloader.class);
    private static final InsertConfigReloader instance = new InsertConfigReloader();
    private Map<String, InsertConfig> configCache = new ConcurrentHashMap<>();

    private InsertConfigReloader() {
    }


    @Override
    public void reloadFileUpdate(Path path) {
        String json = "";
        InsertConfig insertConfig = null;
        try {
            json = FileUtil.readToString(path);
            insertConfig = JSONObject.parseObject(json, InsertConfig.class);
            insertConfig.init();
            String configName = path.getFileName().toString();
            if (validParam(insertConfig, configName)) {
                configCache.put(configName, insertConfig);
            }
        } catch (IOException e) {
            logger.error("read file error path={} ", path.toString(), e);
        } finally {
            if (insertConfig != null) {
                logger.error("reload file path={},insertsql={}{}", path.toString(), String.format(insertConfig.getInsertSql(), insertConfig.getTableName()), insertConfig.getInsertValues());
            } else {
                logger.error("reload file path={},content={}", path.toString(), json);
            }
        }
    }

    public static InsertConfigReloader getInstance() {
        return instance;
    }

    private Map<String, InsertConfig> getConfigCache() {
        return configCache;
    }

    public InsertConfig getInsertEntryConfig(String configName) {
        return getInstance().getConfigCache().get(configName);
    }

    private boolean validParam(InsertConfig insertConfig, String configName) {
        if (insertConfig.getDataSourceName() == null || insertConfig.getTableName() == null) {
            logger.error("config param is null error  databaseName={},tableName={}", insertConfig.getDataSourceName(), insertConfig.getTableName());
            return false;
        }
        if (insertConfig.getLogType() == null || (!InsertConfig.JSON_LOG_TYPE.equals(insertConfig.getLogType()) && !InsertConfig.SPLIT_LOG_TYPE.equals(insertConfig.getLogType()))) {
            logger.error("logtype  is not valid error must be json or split");
            return false;
        }
        if (insertConfig.getColumns() == null || insertConfig.getColumns().isEmpty()) {
            logger.error("column is null error config name {}", configName);
            return false;
        }
        return true;
    }
}
