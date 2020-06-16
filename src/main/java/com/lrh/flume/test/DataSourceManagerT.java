package com.lrh.flume.test;

import com.lrh.flume.datasource.ConnectionProxy;
import com.lrh.flume.datasource.DataSourceManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * @version 1.0
 * @auther lironghui
 * @date 2020/6/14
 */
public class DataSourceManagerT {

    public static void main(String[] args) throws SQLException {
        DataSourceManager.start();
        testGetConnection();
        LockSupport.park();

    }

    /**
     * 获取链接性能测试 8个线程获取和归还jdbc链接 100万次 耗时4秒
     */
    private static void testGetConnection() {
        String datasourceName = "lrh_db";
        int count = 1000000;
        long start = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletableFuture[] completableFutureArray = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            completableFutureArray[i] = CompletableFuture.runAsync(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection connection = DataSourceManager.getConnection(datasourceName);
                        if (connection != null) {
                            connection.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, executorService);
        }
        long end = System.currentTimeMillis();
        CompletableFuture.allOf(completableFutureArray).join();
        System.out.println("execute count=" + count + "；time=" + (end - start));
    }
}
