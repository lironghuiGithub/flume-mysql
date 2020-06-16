package com.lrh.flume.util;

import com.lrh.flume.sql.model.InsertColumnConfig;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 解析日志内容成JDBC TYPE
 * preparedStatement.setTimestamp() ;-->timestamp
 * preparedStatement.setInt();     -->int
 * preparedStatement.setString();  -->String
 */
public class SqlUtil {
    PreparedStatement preparedStatement;
    //当前时间默认值
    private static final String DATE_VALUE_NOW = "now()";
    private static final String DATE_FORMAT_TIMESTAMP = "timestamp";

    public static Object converToMyqlType(String content, InsertColumnConfig columnConfig) {
        String validValue = content;
        //如果为空使用默认值
        if (content == null || content.isEmpty()) {
            if (columnConfig.getDufaultValue() != null) {
                validValue = columnConfig.getDufaultValue();
            }
        } else {
            //截取长度
            int validLenth = columnConfig.getLength();
            if (validLenth > 0 && validValue.length() > validLenth) {
                validValue = validValue.substring(0, validLenth);
            }
        }
        return converToMyqlType(validValue, columnConfig.getJdbcType(), columnConfig.getDateFormat());
    }

    public static Object converToMyqlType(String content, String jdbcType, String dateFormat) {

        if ("char".equalsIgnoreCase(jdbcType)) {
            return content;
        } else if ("varchar".equalsIgnoreCase(jdbcType)) {
            return content;
        } else if ("text".equalsIgnoreCase(jdbcType)) {
            return content;
        } else if ("longtext".equalsIgnoreCase(jdbcType)) {
            return content;
        }
        if (content == null || content.isEmpty()) {
            return content;
        }
        if ("tinyint".equalsIgnoreCase(jdbcType)) {
            return Short.parseShort(content);
        } else if ("smallint".equalsIgnoreCase(jdbcType)) {
            return Integer.parseInt(content);
        } else if ("int".equalsIgnoreCase(jdbcType)) {
            return Integer.parseInt(content);
        } else if ("bigint".equalsIgnoreCase(jdbcType)) {
            return Long.parseLong(content);
        } else if ("float".equalsIgnoreCase(jdbcType)) {
            return Float.parseFloat(content);
        } else if ("double".equalsIgnoreCase(jdbcType)) {
            return Double.parseDouble(content);
        } else if ("decimal".equalsIgnoreCase(jdbcType)) {
            return new BigDecimal(content);
        } else if ("datetime".equalsIgnoreCase(jdbcType)) {
            return buildTimestamp(content, dateFormat);
        } else if ("timestmp".equalsIgnoreCase(jdbcType)) {
            return buildTimestamp(content, dateFormat);
        }
        throw new IllegalArgumentException("UNSUPPORT JDBC TYPE " + jdbcType);
    }

    /**
     * 根据配置生产时间戳
     *
     * @param content
     * @param dateFormat
     * @return
     */
    private static Timestamp buildTimestamp(String content, String dateFormat) {
        if (DATE_VALUE_NOW.equalsIgnoreCase(content)) { //“now()” 解析为当前时间
            return Timestamp.from(Instant.now());
        } else if (DATE_FORMAT_TIMESTAMP.equalsIgnoreCase(dateFormat)) { //解析时间戳 nginx时间格式特殊支持
            if (content.indexOf(".") > 0) {//nginx $msc 为1592317599.569
                content = content.replace(".", "");
            }
            return Timestamp.from(Instant.ofEpochMilli(Long.parseLong(content)));
        } else {  //根据配置dateFormat解析时间
            return Timestamp.from(LocalDateTime.parse(content, DateTimeFormatter.ofPattern(dateFormat)).atZone(ZoneId.systemDefault()).toInstant());
        }
    }

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        String content = "1592317599.569";
        Timestamp timestamp = buildTimestamp(content, DATE_FORMAT_TIMESTAMP);
        System.out.println(timestamp);
    }
}
