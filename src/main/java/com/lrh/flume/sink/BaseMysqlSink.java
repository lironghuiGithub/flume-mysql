package com.lrh.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.lrh.flume.datasource.ConnectionProxy;
import com.lrh.flume.datasource.DataSourceManager;
import com.lrh.flume.sql.InsertConfigManager;
import com.lrh.flume.sql.model.InsertConfig;
import com.lrh.flume.sql.model.InsertItem;
import com.lrh.flume.util.DateUtil;
import com.lrh.flume.util.NamedThreadFactory;
import com.lrh.flume.util.PatternUtil;
import com.zaxxer.hikari.HikariDataSource;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;

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
    //线程池数组 同一张表使用同一个线程池共享一个connection
    protected ExecutorService[] executorServiceArray;
    //每个库每个表使用一个数据库链接
    protected Map<String, ConnectionProxy> connectionCache;
    //创建connection的锁
    protected Object LOCK = new Object();

    @Override
    public synchronized void start() {
        super.start();
        DataSourceManager.start();
        InsertConfigManager.start();
        connectionCache = new HashMap<>();
        executorServiceArray = new ExecutorService[threadNum];
        for (int i = 0; i < threadNum; i++) {
            //单线程线程池避免多线程并发使用 jdbc connection
            executorServiceArray[i] = Executors.newSingleThreadExecutor(new NamedThreadFactory(this.getClass().getSimpleName()));
        }
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
                ConnectionProxy connection = DataSourceManager.getConnection(insertConfig.getDataSourceName());
                //不分库分表直接存入缓存 减少一次取connection的次数
                if (!insertConfig.isTableNeedFreemarkerParse() && !insertConfig.isTableNeedDateParse()) {
                    connectionCache.put(buildConnectionKey(insertConfig.getDataSourceName(), insertConfig.getTableName()), connection);
                } else {
                    //分库或者分表情况这个connection直接返回链接池
                    connection.close();
                }
            }
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {//对事件进行处理
                    String line = new String(event.getBody());
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
            insertIntoMysql(insertConfig, insertItemCache, insertFutureList);
            //虽然插入是并发插入 单独flume每个执行批次不能并发插入 防止内存中太多InsertItem 导致内存溢出
            if (insertFutureList.size() > 0) {
                CompletableFuture.allOf(insertFutureList.toArray(new CompletableFuture[insertFutureList.size()])).join();
            }
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            insertFutureList.clear();
            insertItemCache.clear();
            //每次入库完成必须归还connection
            if (!connectionCache.isEmpty()) {
                for (ConnectionProxy connection : connectionCache.values()) {
                    connection.close();
                }
            }
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
    private void insertIntoMysql(InsertConfig insertConfig, Map<String, List<InsertItem>> insertItemCache, List<CompletableFuture> insertFutureList) {
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
        InsertItem firstItem = insertItemList.get(0);
        String realDataSourceName = firstItem.getDataSourceName();
        if (insertConfig.isDataSourceNeedDateParse()) {
            realDataSourceName = parseDateTime(realDataSourceName);
        }
        String realTableName = firstItem.getTableName();
        if (insertConfig.isTableNeedDateParse()) {
            realTableName = parseDateTime(realTableName);
        }
        ConnectionProxy connection;
        try {
            connection = createConnection(realDataSourceName, realTableName);
        } catch (SQLException e) {
            logger.error("create Connection error");
            return null;
        }
        int hash = (int) connection.getConnectionId() % threadNum;
        CompletableFuture future = CompletableFuture.runAsync(new InsertIntoMysqlRun(insertItemList, insertConfig, realTableName, connection), executorServiceArray[hash]).whenComplete(new BiConsumer<Void, Throwable>() {
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
        private List<InsertItem> insertItemList;
        private InsertConfig insertConfig;
        private String realTableName;
        private ConnectionProxy connection;

        public InsertIntoMysqlRun(List<InsertItem> insertItemList, InsertConfig insertConfig, String realTableName, ConnectionProxy connectionProxy) {
            this.insertItemList = insertItemList;
            this.insertConfig = insertConfig;
            this.realTableName = realTableName;
            this.connection = connectionProxy;
        }

        @Override
        public void run() {
            PreparedStatement preparedStatement = null;
            try {
                String insertSQL = buidInertSQL(insertItemList, insertConfig);
                insertSQL = String.format(insertSQL, realTableName);
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
                insertItemList.clear();
            }
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
//        logger.error("build sql {}", sb.toString());
        return sb.toString();
    }

    /**
     * 日表月表等需要替换成当前时间 yyyyMMdd ->2020-06-03
     *
     * @param str
     * @return
     */
    private String parseDateTime(String str) {
        Matcher matcher = PatternUtil.dateFormat.matcher(str);
        if (matcher.find()) {
            //[yyyyMMdd] -->20200615
            return str.replace(matcher.group(0), DateUtil.format(new Date(), matcher.group(1)));
        }
        return str;
    }

    /**
     * 库名`表名作为一个key共享一个connection
     *
     * @param datasourceName
     * @param tableName
     * @return
     */
    private String buildConnectionKey(String datasourceName, String tableName) {
        return new StringBuilder().append(datasourceName).append("`").append(tableName).toString();
    }

    /**
     * 库名`表名作为一个key共享一个connection
     *
     * @param datasourceName
     * @param tableName
     * @return
     */
    private ConnectionProxy createConnection(String datasourceName, String tableName) throws SQLException {
        String connectionKey = buildConnectionKey(datasourceName, tableName);
        ConnectionProxy connection = connectionCache.get(connectionKey);
        if (connection != null) {
            return connection;
        }
        //创建数据源上锁， 防止重复创建数据源
        synchronized (LOCK) {
            connection = connectionCache.get(connectionKey);
            if (connection != null) {
                return connection;
            } else {
                connection = DataSourceManager.getConnection(datasourceName);
                connectionCache.put(connectionKey, connection);
            }
        }
        return connection;
    }
}
