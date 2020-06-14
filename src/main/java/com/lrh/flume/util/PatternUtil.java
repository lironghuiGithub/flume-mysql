package com.lrh.flume.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternUtil {
    //${aa}
    private static final Pattern freemarker$ = Pattern.compile("\\$\\{\\S+\\}");
    //[yyyyMMdd]  [yyyyMM]   [yyyy_MM]
    private static final Pattern dateFormat = Pattern.compile("\\[(yyyy\\S+?)\\]");
    //数字包含0
    private static final Pattern number_with_zero = Pattern.compile("[0-9]+");

    /**
     * 判断是否包含${xxx}
     *
     * @param string
     * @return
     */
    public static boolean isFreemarkerLang(String string) {
        if (string == null || string.length() == 0) {
            return false;
        }
        return freemarker$.matcher(string).find();
    }

    public static boolean isDateFormat(String string) {
        if (string == null || string.length() == 0) {
            return false;
        }
        return dateFormat.matcher(string).find();
    }

    public static String getDateFormat(String string) {
        Matcher matcher = dateFormat.matcher(string);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public static boolean isNumber(String string) {
        if (string == null || string.length() == 0) {
            return false;
        }
        return number_with_zero.matcher(string).find();
    }

    public static void main(String[] args) {
        System.out.println(isNumber("123"));
        System.out.println(isNumber("adb"));
        test();
    }

    private static void test() {
        int len = 1000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            String string = "gc_act[yyyyMMdd][ssssssss]";
            Matcher matcher = dateFormat.matcher(string);
            matcher.find();
            matcher.group(1);
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
