package com.lrh.flume.sql.model;

import com.lrh.flume.util.PatternUtil;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * {name} json 默认 去json中的key， split默认取数组中下标
 * [yyyyMMdd] []表示嵌套的时间格式
 */
public class InsertConfig {

    private static final int DEFAULT_BATCH_SIZE = 500;
    public static final String JSON_LOG_TYPE = "json";
    public static final String SPLIT_LOG_TYPE = "split";

    /**
     * 日志格式 json or split（|）
     */
    private String logType;
    /**
     * 分割符 暂时不支持配置 默认‘|’
     */
//    private String delimiter=DEFAULT_DELIMITER;
    /**
     * 数据库名 gc_db{ds1}[yyyyMMdd]
     */
    private String dataSourceName;
    /**
     * 每次批量插入数据
     */
    private int batchSize = DEFAULT_BATCH_SIZE;

    /**
     * 表明 gc_act{ds1}[yyyyMMdd]
     */
    private String tableName;
    /**
     * 字段
     */
    private List<InsertColumnConfig> columns;
    /*******************下列字段由配置生成 不需要配置************************/

    /**
     * sql: INSERT INTO sysConfig (s_key,s_value,d_create) VALUES
     */
    private String insertSql;
    /**
     * (?,?,?,?,now())
     */
    private String insertValues;

    /**
     * 非now() 需要参数的列
     */
    private InsertColumnConfig[] insertParamColumnArray;

    /**
     * 数据库需要使用freemarker解析 ${key}
     */
    private boolean dataSourceNeedFreemarkerParse;
    /**
     * 数据库需要解析时间包含[yyyyMMdd]
     */
    private boolean dataSourceNeedDateParse;
    /**
     * 表名需要使用freemarker解析 ${key}
     */
    private boolean tableNeedFreemarkerParse;
    /**
     * 表名需要解析时间包含[yyyyMMdd]
     */
    private boolean tableNeedDateParse;

    public void init() {
        this.setDataSourceNeedFreemarkerParse(PatternUtil.isFreemarkerLang(this.getDataSourceName()));
        this.setDataSourceNeedDateParse(PatternUtil.isDateFormat(this.getDataSourceName()));
        this.setTableNeedFreemarkerParse(PatternUtil.isFreemarkerLang(this.getTableName()));
        this.setTableNeedDateParse(PatternUtil.isDateFormat(this.getTableName()));
        buildSQL(this);
    }

    private void buildSQL(InsertConfig insertConfig) {
        List<InsertColumnConfig> paramColumnList = new ArrayList<>();
        // INSERT INTO sys_config (`n_id`, `s_key`, `s_value`, `d_create`) VALUES ('1', 'dbName', 'lrh_db', '2020-06-05 23:53:45');
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO");
        insertSql.append(" ").append("%s"); //分表表明不固定需要运行时指定
        insertSql.append(" ").append("(");
        List<InsertColumnConfig> columns = insertConfig.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            InsertColumnConfig column = columns.get(i);
            if (i == 0) {
                insertSql.append(column.getColunmName());
            } else {
                insertSql.append(",");
                insertSql.append(column.getColunmName());
            }
        }
        insertSql.append(")");
        insertSql.append(" ").append("VALUES ");
        insertConfig.setInsertSql(insertSql.toString());

        StringBuilder insertValues = new StringBuilder();
        insertValues.append(" ").append("(");
        for (int i = 0; i < columns.size(); i++) {
            InsertColumnConfig config = columns.get(i);
            if (i != 0) {
                insertValues.append(",");
            }
            if (config.isUserNow()) {
                insertValues.append("now()");
            } else {
                insertValues.append("?");
                paramColumnList.add(config);
            }
        }
        insertValues.append(")");
        insertConfig.setInsertValues(insertValues.toString());

        insertConfig.setInsertParamColumnArray(paramColumnList.toArray(new InsertColumnConfig[paramColumnList.size()]));
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<InsertColumnConfig> getColumns() {
        return columns;
    }

    public void setColumns(List<InsertColumnConfig> columns) {
        this.columns = columns;
    }

    public String getInsertSql() {
        return insertSql;
    }

    public void setInsertSql(String insertSql) {
        this.insertSql = insertSql;
    }

    public String getInsertValues() {
        return insertValues;
    }

    public void setInsertValues(String insertValues) {
        this.insertValues = insertValues;
    }

    public InsertColumnConfig[] getInsertParamColumnArray() {
        return insertParamColumnArray;
    }

    public void setInsertParamColumnArray(InsertColumnConfig[] insertParamColumnArray) {
        this.insertParamColumnArray = insertParamColumnArray;
    }

    public boolean isDataSourceNeedFreemarkerParse() {
        return dataSourceNeedFreemarkerParse;
    }

    public void setDataSourceNeedFreemarkerParse(boolean dataSourceNeedFreemarkerParse) {
        this.dataSourceNeedFreemarkerParse = dataSourceNeedFreemarkerParse;
    }

    public boolean isDataSourceNeedDateParse() {
        return dataSourceNeedDateParse;
    }

    public void setDataSourceNeedDateParse(boolean dataSourceNeedDateParse) {
        this.dataSourceNeedDateParse = dataSourceNeedDateParse;
    }

    public boolean isTableNeedFreemarkerParse() {
        return tableNeedFreemarkerParse;
    }

    public void setTableNeedFreemarkerParse(boolean tableNeedFreemarkerParse) {
        this.tableNeedFreemarkerParse = tableNeedFreemarkerParse;
    }

    public boolean isTableNeedDateParse() {
        return tableNeedDateParse;
    }

    public void setTableNeedDateParse(boolean tableNeedDateParse) {
        this.tableNeedDateParse = tableNeedDateParse;
    }
}
