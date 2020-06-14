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
 * preparedStatement.setTimestamp() ;-->Timestamp
 * preparedStatement.setInt();     -->int
 * preparedStatement.setString();  -->String
 */
public class SqlUtil {
    PreparedStatement preparedStatement;
    //当前时间默认值
    private static final String DEAULT_NOW_VALUE = "now()";

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
            //如果默认值是 “now()” 则取当前时间
            if (DEAULT_NOW_VALUE.equalsIgnoreCase(content)) {
                return Timestamp.from(Instant.now());
            }
            return Timestamp.from(LocalDateTime.parse(content, DateTimeFormatter.ofPattern(dateFormat)).atZone(ZoneId.systemDefault()).toInstant());
        } else if ("timestmp".equalsIgnoreCase(jdbcType)) {
            if (DEAULT_NOW_VALUE.equalsIgnoreCase(content)) {
                return Timestamp.from(Instant.now());
            }
            return Timestamp.from(LocalDateTime.parse(content, DateTimeFormatter.ofPattern(dateFormat)).atZone(ZoneId.systemDefault()).toInstant());
        }
        throw new IllegalArgumentException("UNSUPPORT JDBC TYPE " + jdbcType);
    }


}
