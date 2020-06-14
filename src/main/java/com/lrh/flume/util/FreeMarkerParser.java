package com.lrh.flume.util;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.internal.$Gson$Preconditions;
import com.lrh.flume.datasource.DataSourceManager;
import freemarker.cache.StringTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.logging.log4j.core.util.ExecutorServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;


public class FreeMarkerParser {
    private static Logger logger = LoggerFactory.getLogger(FreeMarkerParser.class);
    private static Configuration configuration;
    private static final String DEFAULT_ENCODDING = "utf-8";
    private static final AtomicBoolean init = new AtomicBoolean();
    private static final Object LOCK = new Object();

    static {
        init();
    }

    private static void init() {
        if (!init.compareAndSet(false, true)) {
            logger.error("free marker parse init already");
        }
        configuration = new Configuration(Configuration.getVersion());
        configuration.setDefaultEncoding("utf-8");
        StringTemplateLoader stringTemplateLoader = new StringTemplateLoader();
        configuration.setTemplateLoader(stringTemplateLoader);


    }

    private static Template createTemplate(String name, String templateContent) throws IOException {
        Template template = configuration.getTemplate(name, Locale.CHINA, null, DEFAULT_ENCODDING, true, true);
        if (template != null) {
            return template;
        }
        synchronized (LOCK) { // 以下操作不是线程安全，要加上同步
            // 获取模板加载器
            StringTemplateLoader templateLoader = (StringTemplateLoader) configuration.getTemplateLoader();
            // 如果加载器已经是字符串加载器，则在原来的加载器上put一个新的模板
            //缓存使用的是hashMap 添加模板必须上锁
            templateLoader.putTemplate(name, templateContent);
            configuration.setTemplateLoader(templateLoader);
            // 这里要清一下缓存，不然下面可能会获取不到模板
            configuration.clearTemplateCache();
            template = configuration.getTemplate(name, "utf-8");
            return template;
        }
    }

    public static String parseJson(String templateContent, Map dataMap) {
        StringWriter out = new StringWriter();
        Template template = null;
        try {
            template = createTemplate(templateContent, templateContent);
            template.process(dataMap, out);
        } catch (Exception e) {
            logger.error("createTemplate error templateName={}", templateContent);
        }
        return out.toString();
    }

    public static String parseArray(String templateContent, String[] array) {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("array", array);
        return parseJson(templateContent, dataMap);
    }

    public static void main(String[] args) throws Exception {
        test1();
        test2();
    }

    /**
     * 压力测试
     * 100万需要4S
     */
    private static void test1() {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("a", "aaaa");
        dataMap.put("b", "bbbb");

        int len = 10000;
        CompletableFuture[] futures = new CompletableFuture[len];
        ExecutorService executors = Executors.newFixedThreadPool(8);
        long start = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            int sufx = i % 4;
            String templateName="gc_act" + sufx + "${a}${b}${a}";
            CompletableFuture future = CompletableFuture.runAsync(new Runnable() {
                @Override
                public void run() {
                    System.out.println(parseJson(templateName,dataMap));
                }
            }, executors);
            futures[i] = future;
        }
        CompletableFuture.allOf(futures).join();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    /**
     * 压力测试
     * 100万需要4S
     */
    private static void test2() throws Exception {
        String[] arr = new String[10];
        for (int i = 0; i < 10; i++) {
            arr[i] = "" + i;
        }
        String templateName="table${array[5]}[yyyyMMdd]";

        System.out.println(parseArray(templateName,arr));

    }
}
