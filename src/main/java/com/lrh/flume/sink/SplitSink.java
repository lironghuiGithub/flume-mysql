package com.lrh.flume.sink;

import com.lrh.flume.sql.InsertConfigManager;
import com.lrh.flume.sql.model.InsertColumnConfig;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.sql.model.InsertItem;
import com.lrh.flume.util.FreeMarkerParser;
import com.lrh.flume.util.SqlUtil;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 *
 */

/**
 * @auther lironghui
 * @date 2020/6/13
 * 分割方式日志 解析日志内容
 * 返回库名+表名 +sql 参数
 */
public class SplitSink extends BaseMysqlSink {
    @Override
    public InsertItem converInsertItemtFromContent(String line, InsertConfig insertConfig) {
        if (!InsertConfig.SPLIT_LOG_TYPE.equals(insertConfig.getLogType())) {
            logger.error("··············· this is SplitSink but config logType is {}", insertConfig);
            return null;
        }
        //databaseName 和tableName 转换成真实的值， 时间转换放在入库时转换减少转换时间消耗
        String[] paramsArray = line.split("\\|", -1);
        String dataSourceName = insertConfig.getDataSourceName();
        if (insertConfig.isDataSourceNeedFreemarkerParse()) {
            dataSourceName = FreeMarkerParser.parseArray(dataSourceName, paramsArray);
        }
        String tableName = insertConfig.getTableName();
        if (insertConfig.isTableNeedFreemarkerParse()) {
            tableName = FreeMarkerParser.parseArray(tableName, paramsArray);
        }
        InsertColumnConfig[] insertParamColumnArray = insertConfig.getInsertParamColumnArray();
        int maxParamIndex = paramsArray.length - 1;
        int columnLength = insertParamColumnArray.length;
        Object[] sqlValues = new Object[columnLength];
        for (int i = 0; i < columnLength; i++) {
            InsertColumnConfig columnConfig = insertParamColumnArray[i];
//          解析日志内容 对index out bound做验证
            if (columnConfig.getIndex() > maxParamIndex) {
                logger.error("index out bound error columnName={},index={}, maxIndex={},line={}", columnConfig.getColunmName(), columnConfig.getIndex(), maxParamIndex, line);
                return null;
            }
            String content = paramsArray[columnConfig.getIndex()];
//            对不允许为空的字段如果为空则认为垃圾数据进行丢弃
            if (!columnConfig.isNullable() && content.isEmpty()) {
                logger.error("column must not be null name={}", columnConfig.getColunmName());
                return null;
            }
//            把字符串转换成对应的JdbcType
            sqlValues[i] = SqlUtil.converToMyqlType(content, columnConfig);
        }
        InsertItem insertItem = new InsertItem();
        insertItem.setDataSourceName(dataSourceName);
        insertItem.setTableName(tableName);
        insertItem.setInsertValues(sqlValues);
        return insertItem;
    }

}
