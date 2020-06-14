package com.lrh.flume.datasource;

import org.apache.velocity.texen.util.PropertiesUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DataSourceConfig {
    private static final int default_minimumIdle = 5;
    private static final int default_maximumPoolSize = 20;
    private static final long default_idleTimeout = 600000;
    private static final long default_maxLifetime = 1800000;
    /**
     * 链接池的名称 需要保证唯一 默认文件名
     */
    private String dataSourceName;
    private String driverClassName;
    private String jdbcUrl;
    private String userName;
    private String password;
    //minimumIdle池中维护的最小空闲连接数	10	minIdle<0或者minIdle>maxPoolSize,则被重置为maxPoolSize
    private int minimumIdle = default_minimumIdle;
    // maximumPoolSize	池中最大连接数，包括闲置和使用中的连接	10	如果maxPoolSize小于1，则会被重置。当minIdle<=0被重置为DEFAULT_POOL_SIZE则为10;如果minIdle>0则重置为minIdle的值
    private int maximumPoolSize = default_maximumPoolSize;
    // idleTimeout	连接允许在池中闲置的最长时间	600000	如果idleTimeout+1秒>maxLifetime 且 maxLifetime>0，则会被重置为0（代表永远不会退出）；如果idleTimeout!=0且小于10秒，则会被重置为10秒
    private long idleTimeout = default_idleTimeout;
    //maxLifetime	池中连接最长生命周期	1800000	如果不等于0且小于30秒则会被重置回30分钟
    private long maxLifetime = default_maxLifetime;

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMinimumIdle() {
        return minimumIdle;
    }

    public void setMinimumIdle(int minimumIdle) {
        this.minimumIdle = minimumIdle;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public long getMaxLifetime() {
        return maxLifetime;
    }

    public void setMaxLifetime(long maxLifetime) {
        this.maxLifetime = maxLifetime;
    }
}
