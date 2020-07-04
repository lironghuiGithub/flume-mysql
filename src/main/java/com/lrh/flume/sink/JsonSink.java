package com.lrh.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.lrh.flume.sql.model.InsertColumnConfig;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.sql.model.InsertItem;
import com.lrh.flume.util.FreeMarkerParser;
import com.lrh.flume.util.SqlUtil;

/**
 *
 */

/**
 * @auther lironghui
 * @date 2020/6/13
 * JSON日志 解析日志内容
 * 返回库名+表名 +sql 参数
 */
public class JsonSink extends BaseMysqlSink {
    @Override
    public InsertItem converInsertItemtFromContent(String line, InsertConfig insertConfig) {
        if (!InsertConfig.JSON_LOG_TYPE.equals(insertConfig.getLogType())) {
            logger.error("··············· this is jsonSink but config logType is {}", insertConfig);
            return null;
        }
        JSONObject jsonObject = JSONObject.parseObject(line);
        String dataSourceName = insertConfig.getDataSourceName();
        if (insertConfig.isDataSourceNeedFreemarkerParse()) {
            dataSourceName = FreeMarkerParser.parseJson(dataSourceName, jsonObject);
        }
        String tableName = insertConfig.getTableName();
        if (insertConfig.isTableNeedFreemarkerParse()) {
            tableName = FreeMarkerParser.parseJson(tableName, jsonObject);
        }
        InsertColumnConfig[] insertParamColumnArray = insertConfig.getInsertParamColumnArray();
        int columnLength = insertParamColumnArray.length;
        Object[] sqlValues = new Object[columnLength];
        for (int i = 0; i < columnLength; i++) {
            InsertColumnConfig columnConfig = insertParamColumnArray[i];
            String content = jsonObject.getString(columnConfig.getJsonKey());
//            对不允许为空的字段如果为空则认为垃圾数据进行丢弃
            if (!columnConfig.isNullable() && (content == null||content.isEmpty())) {
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
