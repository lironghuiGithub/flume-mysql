package com.lrh.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.lrh.flume.datasource.DataSourceManager;
import com.lrh.flume.sql.InsertConfigManager;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.sql.model.InsertItem;
import com.lrh.flume.util.DateUtil;
import com.lrh.flume.util.PatternUtil;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * 一个sink处理一个业务父类
 * 指定配置文件名称
 */
abstract class BaseMysqlSink extends AbstractSink implements Configurable {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static final int DEFAULT_BATCH_SIZE = 2000;
    protected static final int DEFAULT_THREAD_NUM = 5;
    //批量拉去channel数据大小
    protected int batchSize;
    //每个业务json对应一个配置文件
    protected String configName;
    //每个业务json对应一个配置文件，配置文件名称在json中的值
    protected int threadNum;

    protected ExecutorService executorService;


    @Override
    public synchronized void start() {
        super.start();
        DataSourceManager.start();
        InsertConfigManager.start();
        executorService = Executors.newFixedThreadPool(threadNum);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        //防止变动和删除 采用每次现取现用
        InsertConfig insertConfig = InsertConfigManager.getInsertEntryConfig(configName);
        if (insertConfig == null) {
            logger.error("config not exist error configName {}", configName);
            result = Status.BACKOFF;
            return result;
        }
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        Map<String, List<InsertItem>> insertItemCache = new HashMap<>();
        List<CompletableFuture> insertFutureList = new ArrayList<>();
        try {
            //不需要分库的情况，提前获取一次connecttion 用于检测数据库链接是否可用，防止数据库挂掉数据丢失
            if (!insertConfig.isDataSourceNeedFreemarkerParse() && !insertConfig.isDataSourceNeedDateParse()) {
                Connection connection = DataSourceManager.getConnection(insertConfig.getDataSourceName());
                if (connection != null) {
                    connection.close();
                }
            }
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {//对事件进行处理
                    String line = new String(event.getBody());
                    logger.info("line:"+line);
                    if (line == null || line.isEmpty()) {
                        continue;
                    }
                    InsertItem insertItem = null;
                    try {
                        insertItem = converInsertItemtFromContent(line, insertConfig);
                    } catch (Exception e) {
                        logger.error("line={},configName={}", line, configName, e);
                    }
                    insertIntoCache(insertItem, insertConfig, insertItemCache, insertFutureList);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            /**
             * 未达到批量数量的数据入库
             */
            insertIntoMysqlAsync(insertConfig, insertItemCache, insertFutureList);
            //虽然插入是并发插入 单独flume每个执行批次不能并发插入 防止内存中太多InsertItem 导致内存溢出
            if (insertFutureList.size() > 0) {
                CompletableFuture.allOf(insertFutureList.toArray(new CompletableFuture[insertFutureList.size()])).join();
            }
            insertItemCache.clear();
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            //异常数据直接抛弃不做回滚操作  分库分表的做法部分库链接不上没法全部回滚
            try {
                transaction.commit();
            } catch (Exception e) {
                logger.error("", e);
            }
            transaction.close();
        }
        return result;
    }


    @Override
    public void configure(Context context) {
        batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);
        threadNum = context.getInteger("threadNum", DEFAULT_THREAD_NUM);
        configName = context.getString("configName");
        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive number!!");
        Preconditions.checkArgument(threadNum > 0, "threadNum must be a positive number!!");
        Preconditions.checkNotNull(configName, "configName must not be null");
    }

    /**
     * 对象现存入cache等到了批量入库的batchSize再入库
     *
     * @param insertItem
     */
    private void insertIntoCache(InsertItem insertItem, InsertConfig insertConfig, Map<String, List<InsertItem>> insertItemCache, List<CompletableFuture> insertFutureList) {
        if (insertItem == null) {
            return;
        }
        String cacheKey = new StringBuilder().append(insertItem.getDataSourceName()).append("`").append(insertItem.getTableName()).toString();
        List<InsertItem> insertItemList = insertItemCache.get(cacheKey);
        if (insertItemList == null) {
            insertItemList = new ArrayList<>(insertConfig.getBatchSize());
        }
        insertItemList.add(insertItem);
        //数量达到批量入库条件则进行入库操作
        if (insertItemList.size() > insertConfig.getBatchSize()) {
            List<InsertItem> insertList = insertItemList;
            insertFutureList.add(insertIntoMysqlAsync(insertList, insertConfig));
            //内部插入后list会clear 外层需要使用新的list
            insertItemList = new ArrayList<>(insertConfig.getBatchSize());
        }
        insertItemCache.put(cacheKey, insertItemList);
    }

    /**
     * 当数量达不到批量插入条件时 需要把现有数据入库
     *
     * @param insertConfig
     * @param insertItemCache
     * @param insertFutureList
     */
    private void insertIntoMysqlAsync(InsertConfig insertConfig, Map<String, List<InsertItem>> insertItemCache, List<CompletableFuture> insertFutureList) {
        for (List<InsertItem> insertItemList : insertItemCache.values()) {
            if (insertItemList.size() > 0) {
                List<InsertItem> insertList = insertItemList;
                insertFutureList.add(insertIntoMysqlAsync(insertList, insertConfig));
            }
        }
    }

    /**
     * 异步入库提交并发插入效率 决绝单个sink性能远低于cahnnel的情况
     *
     * @param insertItemList
     * @param insertConfig
     * @return
     */
    private CompletableFuture insertIntoMysqlAsync(List<InsertItem> insertItemList, InsertConfig insertConfig) {
        CompletableFuture future = CompletableFuture.runAsync(new InsertIntoMysqlRun(insertItemList, insertConfig)).whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void aVoid, Throwable throwable) {
                if (throwable != null) {
                    logger.error("insert into mysql error params={}", JSONObject.toJSONString(insertItemList), throwable);
                }
            }
        });
        return future;

    }

    public abstract InsertItem converInsertItemtFromContent(String content, InsertConfig insertConfig);

    /**
     * 插入数据库采用并发插入默认解决 sink性能赶不上channel导致积压数据问题
     */
    class InsertIntoMysqlRun implements Runnable {
        List<InsertItem> insertItemList;
        InsertConfig insertConfig;

        public InsertIntoMysqlRun(List<InsertItem> insertItemList, InsertConfig insertConfig) {
            this.insertItemList = insertItemList;
            this.insertConfig = insertConfig;
        }

        @Override
        public void run() {
            insertIntoMysql(insertItemList, insertConfig);
        }
    }

    /**
     * 真正插入操作
     *
     * @param insertItemList 需要插入的数据 Objec[]内由子类转化好 varchar Timestamp 等，外层只setObject
     * @param insertConfig   sql配置
     */
    private void insertIntoMysql(List<InsertItem> insertItemList, InsertConfig insertConfig) {
        if (insertItemList.size() < 0) {
            return;
        }
        InsertItem firstItem = insertItemList.get(0);
        String realDataSourceName = firstItem.getDataSourceName();
        if (insertConfig.isDataSourceNeedDateParse()) {
            realDataSourceName = parseDateTime(realDataSourceName);
        }
        String realTableName = firstItem.getTableName();
        if (insertConfig.isTableNeedDateParse()) {
            realTableName = parseDateTime(realTableName);
        }
        String insertSQL = buidInertSQL(insertItemList, insertConfig);
        insertSQL = String.format(insertSQL, realTableName);

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = DataSourceManager.getConnection(realDataSourceName);
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(insertSQL);
            int index = 1;
            for (InsertItem insertItem : insertItemList) {
                Object[] insertValues = insertItem.getInsertValues();
                for (int i = 0; i < insertValues.length; i++) {
                    preparedStatement.setObject(index++, insertValues[i]);
                }
            }
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            logger.error("", e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    logger.error("", e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("", e);
                }
            }
            insertItemList.clear();
        }
    }

    /**
     * 根据数量拼装SQL INSERT INTO sysConfig (s_key,s_value,d_create) VALUES  (?,?,now()),(?,?,now()),(?,?,now()),(?,?,now())
     *
     * @param insertItemList
     * @param insertConfig
     * @return
     */
    private String buidInertSQL(List<InsertItem> insertItemList, InsertConfig insertConfig) {
        StringBuilder sb = new StringBuilder();
        sb.append(insertConfig.getInsertSql());
        for (int i = 0; i < insertItemList.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(insertConfig.getInsertValues());
        }
        logger.error("build sql {}", sb.toString());
        return sb.toString();
    }

    /**
     * 日表月表等需要替换成当前时间 yyyyMMdd ->2020-06-03
     *
     * @param str
     * @return
     */
    private String parseDateTime(String str) {
        String dateFormat = PatternUtil.getDateFormat(str);
        if (dateFormat == null || dateFormat.isEmpty()) {
            return str;
        }
        return DateUtil.format(new Date(), str);
    }
}
