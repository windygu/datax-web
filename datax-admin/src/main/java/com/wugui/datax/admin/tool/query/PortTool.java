package com.wugui.datax.admin.tool.query;


import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.wugui.datax.admin.core.util.LocalCacheUtil;
import com.wugui.datax.admin.entity.JobDatasource;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PortTool {
    private int port=0;

    public PortTool(JobDatasource jobDatasource) throws IOException {
        String url=jobDatasource.getJdbcUrl();
    }
    public static boolean scan(String host, int port) {
        return scan(host,port,5000);
    }
    public static boolean scan(String host, int port, int timeOut) {
        boolean flag = false;
        Socket socket = null;
        try {
            socket = new Socket(host,port);
            socket.setSoTimeout(timeOut);
            flag = true;
        } catch (IOException e) {
            // e.printStackTrace();
        } finally{
            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (Exception e) {
            }
        }
        return flag;
    }
    public static void main(String[] args) {
        String host = "bigdata2";
        System.out.println(scan(host,9092));
//        for (int i = 1; i <= 65535; i++) {
//            if (scan(host, i, 5000)) {
//                System.out.println("PORT listening:" + i);
//            }else{
//                System.out.println(i);
//            }
//        }
    }
}
