package com.lrh.flume.sql.model;

/**
 * 数据库字段配置
 */
public class InsertColumnConfig {
    //json 模式的jsonkey字段字段名
    private String jsonKey;
    //split 模式的下标
    private int index;
    //mysql column
    private String colunmName;
    //jdbc type
    private String jdbcType;
    //默认值
    private String dufaultValue;
    //字段长度
    private int length;
    // 字段是字符串时 转时间类型 yyyy-MM-dd hh:mm:ss
    private String dateFormat;
    //如果字段是null 则丢弃整整行数据 假设按照产品分表 产品字段为null 则是垃圾数据需要丢弃
    private boolean nullable = true;
    //是否使用当前时间 now();
    private boolean userNow = false;

    public String getJsonKey() {
        return jsonKey;
    }

    public void setJsonKey(String jsonKey) {
        this.jsonKey = jsonKey;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getColunmName() {
        return colunmName;
    }

    public void setColunmName(String colunmName) {
        this.colunmName = colunmName;
    }

    public String getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(String jdbcType) {
        this.jdbcType = jdbcType;
    }

    public String getDufaultValue() {
        return dufaultValue;
    }

    public void setDufaultValue(String dufaultValue) {
        this.dufaultValue = dufaultValue;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isUserNow() {
        return userNow;
    }

    public void setUserNow(boolean userNow) {
        this.userNow = userNow;
    }
}
