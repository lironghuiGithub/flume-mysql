package com.lrh.flume.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author lironghui
 * @version 1.0
 * @date 2019/11/24 18:57
 */
public class DateUtil {

    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String MONTH_FORMAT = "yyyy-MM";
    public static final String YYYYMMDD = "yyyyMMdd";
    public static final String YYYYMM = "yyyyMM";
    public static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(TIME_FORMAT);
    public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
    public static final DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern(MONTH_FORMAT);
    public static final long ONE_SECONDS = 1L;
    public static final long MINUTE_OF_SECOND = 60 * ONE_SECONDS;
    public static final long HOUR_OF_SECOND = 60 * MINUTE_OF_SECOND;
    public static final long DAY_OF_SECOND = 24 * HOUR_OF_SECOND;
    public static final long WEEK_OF_SECOND = 7 * DAY_OF_SECOND;
    public static final long MONTH_OF_SECOND = 30 * DAY_OF_SECOND;
    public static final ZoneId DEFAULT_ZONEID = ZoneId.systemDefault();


    /**
     * Date to LocalDateTime
     *
     * @param date
     * @return
     */
    public static LocalDateTime dateToLocalDateTime(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), DEFAULT_ZONEID);
    }

    /**
     * LocalDateTime to Date
     *
     * @param localDateTime
     * @return
     */
    public static Date localDateTimeToDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(DEFAULT_ZONEID).toInstant());
    }

    /**
     * Date to localDate
     *
     * @param date
     * @return
     */
    public static LocalDate dateToLocalDate(Date date) {
        return date.toInstant().atZone(DEFAULT_ZONEID).toLocalDate();
    }

    /**
     * LocalDate to Date
     *
     * @param localDate
     * @return
     */
    public static Date localDateToDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay(DEFAULT_ZONEID).toInstant());
    }

    /******************format  start************************/
    public static String currentTime() {
        return format(LocalDateTime.now(), timeFormatter);
    }

    public static String currentDate() {
        return format(LocalDateTime.now(), dateFormatter);
    }

    public static String currentMonth() {
        return format(LocalDateTime.now(), monthFormatter);
    }

    public static String formatTime(Date date) {
        return format(dateToLocalDateTime(date), timeFormatter);
    }

    public static String formatDate(Date date) {
        return format(dateToLocalDateTime(date), dateFormatter);
    }

    public static String formatMonth(Date date) {
        return format(dateToLocalDateTime(date), monthFormatter);
    }

    public static String format(Date date, String format) {
        return format(dateToLocalDateTime(date), DateTimeFormatter.ofPattern(format));
    }

    public static String format(LocalDateTime localDateTime, String format) {
        return format(localDateTime, DateTimeFormatter.ofPattern(format));
    }

    public static String format(LocalDateTime localDateTime, DateTimeFormatter dateFormatter) {
        return localDateTime.format(dateFormatter);
    }
    /******************format  end************************/
    /******************prase   start************************/
    public static Date parseTime(String time) {
        return parse(time, TIME_FORMAT);
    }

    public static Date parseDate(String time) {
        return parse(time, DATE_FORMAT);
    }

    public static Date parseMonth(String time) {
        return parse(time, MONTH_FORMAT);
    }

    public static Date parse(String time, String format) {
        return parse(time, DateTimeFormatter.ofPattern(format));
    }

    public static Date parse(String time, DateTimeFormatter dateFormatter) {
        LocalDateTime localDateTime = LocalDateTime.parse(time, dateFormatter);
        return localDateTimeToDate(localDateTime);
    }
    /******************prase   end************************/
    /******************Time plus  start************************/
    public static Date plusSeconds(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusSeconds(count));
    }

    public static Date plusMinutes(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusMinutes(count));
    }

    public static Date plusHours(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusHours(count));
    }

    public static Date plusDays(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusDays(count));
    }

    public static Date plusWeeks(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusWeeks(count));
    }

    public static Date plusMonths(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusMonths(count));
    }

    public static Date plusYears(Date date, int count) {
        return localDateTimeToDate(dateToLocalDateTime(date).plusYears(count));
    }
    /******************Time plus  end************************/

    /******************Time diff  start************************/


    public static long diffMillis(Date date, Date date2) {
        return diff(date, date2).toMillis();
    }

    public static long diffSeconds(Date date, Date date2) {
        return diffMillis(date, date2) / 1000;
    }

    public static long diffMinutes(Date date, Date date2) {
        return diff(date, date2).toMinutes();
    }

    public static long diffHours(Date date, Date date2) {
        return diff(date, date2).toHours();
    }

    public static long diffDays(Date date, Date date2) {
        return diff(date, date2).toDays();
    }

    public static Duration diff(Date date, Date date2) {
        return diff(dateToLocalDateTime(date), dateToLocalDateTime(date2));
    }

    public static Duration diff(LocalDateTime localDateTime, LocalDateTime localDateTime2) {
        return Duration.between(localDateTime, localDateTime2);
    }
    /******************Time diff  end************************/
    /**
     * 一天的结束
     */
    public static LocalDateTime endOfDay() {
        return endOfDay(LocalDate.now());
    }

    /**
     * 一天的结束
     */
    public static LocalDateTime endOfDay(LocalDate date) {
        return LocalDateTime.of(date, LocalTime.MAX);
    }

    /**
     * 距离当天结束时间剩下的秒数
     */
    public static long remainSeconds() {
        LocalDateTime localDateTime = LocalDateTime.now();
        LocalDateTime localDateTimeMax = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
        return diff(localDateTime, localDateTimeMax).getSeconds();
    }

    public static void main(String[] args) {

//        Date date = new Date();
//        System.out.println(currentTime());
//        System.out.println(currentDate());
//        System.out.println(currentMonth());
//        System.out.println(formatTime(date));
//        System.out.println(formatDate(date));
//        System.out.println(formatMonth(date));
//        System.out.println(format(date,YYYYMM));
//        System.out.println(formatTime(plusSeconds(date, 1)));
//        System.out.println(formatTime(plusMinutes(date, 1)));
//        System.out.println(formatTime(plusHours(date, 1)));
//        System.out.println(formatTime(plusDays(date, 1)));
//        System.out.println(formatTime(plusWeeks(date, 1)));
//        System.out.println(formatTime(plusMonths(date, 1)));
//        System.out.println(remainSeconds());
        String time = "2019-11-24 21:40:19";
//        Date date2 = parseTime(time);
//        System.out.println(diffMillis(date, date2));
//        System.out.println(diffSeconds(date, date2));
//        System.out.println(diffMinutes(date, date2));
//        System.out.println(diffHours(date, date2));
//        System.out.println(diffDays(date, date2));
        int count = 1000000;
        ExecutorService executorService = Executors.newFixedThreadPool(500);
        LocalDateTime start = LocalDateTime.now();
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            executorService.execute(() -> {
                parseTime(time);
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
            LocalDateTime end = LocalDateTime.now();
            System.out.println(Duration.between(start, end).getSeconds());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
