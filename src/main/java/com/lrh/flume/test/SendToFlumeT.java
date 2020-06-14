package com.lrh.flume.test;

import com.alibaba.fastjson.JSONObject;
import com.lrh.flume.util.DateUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

/**
 * @version 1.0
 * @auther lironghui
 * @date 2020/6/14
 */
public class SendToFlumeT {
    private static final String IP = "127.0.0.1";
    private static final int port = 6666;
    private static final String line = "%s|50081|127.0.0.1|http://www.baidu.com|%s";
    private static final String[] pidArr = {"30", "40", "50"};
    private static final int pidArrLength = pidArr.length;

    public static void main(String[] args) throws Exception {
        int count = 10;
        for (int i = 0; i < count; i++) {
            Socket so = new Socket(IP, port);//端口号要和服务器端相同
            //2.获取输出流，向服务器端发送登录的信息
            OutputStream os = so.getOutputStream();//字节输出流
            PrintWriter pw = new PrintWriter(os);//字符输出流
            BufferedWriter bw = new BufferedWriter(pw);//加上缓冲流
            String pid = pidArr[i % pidArrLength];
            System.out.println(String.format(line, pid, DateUtil.formatTime(new Date())));
            bw.write(String.format(line, pid, DateUtil.formatTime(new Date())));
            bw.flush();
            so.shutdownOutput();//关闭输出流
            //3.关闭资源
            bw.close();
            pw.close();
            os.close();
            so.close();
        }
    }
}
