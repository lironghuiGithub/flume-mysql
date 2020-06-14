package com.lrh.flume.test;

import com.lrh.flume.sink.SplitSink;
import com.lrh.flume.sql.InsertConfigManager;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.sql.model.InsertItem;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * @version 1.0
 * @auther lironghui
 * @date 2020/6/14
 */
public class SplitSinkT {

    public static void main(String[] args) {
        InsertConfigManager.start();
        test();
        LockSupport.park();
    }

    /**
     * 测试性能  100万条数据
     * freemarker 解析表名+库名 耗时8S
     * freemarker 解析表名 耗时6S
     * 不需要freemarker解析 耗时3.7S
     */
    private static void test() {
        String line = "30|50081|127.0.0.1|http://www.baidu.com|2020-01-01 21:54:40";
        String configName = "clickday_split_test.json";
        SplitSink splitSink = new SplitSink();
        InsertConfig insertConfig = InsertConfigManager.getInsertEntryConfig(configName);
        int count = 1000000;
        long start = System.currentTimeMillis();
        System.out.println("cpu :" + Runtime.getRuntime().availableProcessors());
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CompletableFuture[] completableFutureArray = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            completableFutureArray[i] = CompletableFuture.runAsync(new Runnable() {
                @Override
                public void run() {
                    InsertItem insertItem = splitSink.converInsertItemtFromContent(line, insertConfig);
//                    System.out.println(JSONObject.toJSONString(insertItem));
                }
            }, executorService);
        }
        long end = System.currentTimeMillis();
        CompletableFuture.allOf(completableFutureArray).join();
        System.out.println("execute count=" + count + "；time=" + (end - start));
    }
}
