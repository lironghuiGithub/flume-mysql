package com.lrh.flume.sql.model;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertItem {
    private String dataSourceName;

    private String tableName;

    private Object[] insertValues;

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Object[] getInsertValues() {
        return insertValues;
    }

    public void setInsertValues(Object[] insertValues) {
        this.insertValues = insertValues;
    }

    public static void main(String[] args) {
        String reg = " \\S*\\{(\\S)\\}";
        Pattern pattern = Pattern.compile(reg);
        String str = "gc-act{123}";
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            System.out.println(matcher.group());
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
        }
    }
}
