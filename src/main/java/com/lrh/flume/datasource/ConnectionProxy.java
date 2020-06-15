package com.lrh.flume.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * connection代理类 附加id
 *
 * @version 1.0
 * @auther lironghui
 * @date 2020/6/15
 */
public class ConnectionProxy implements Closeable {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final AtomicLong IdGenerater = new AtomicLong();
    //真实的connection链接
    private Connection connection;
    //每次获取connection 生成一个id
    private long connectionId;

    public ConnectionProxy(Connection connection) {
        this.connection = connection;
        connectionId = IdGenerater.getAndIncrement();
    }

    public Connection getConnection() {
        return connection;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        connection.setAutoCommit(autoCommit);
    }

    public void commit() throws java.sql.SQLException {
        connection.commit();
    }

    public PreparedStatement prepareStatement(String sql) throws java.sql.SQLException {
        return connection.prepareStatement(sql);
    }


    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("close connection errror", e);
            }
        }
    }
}
