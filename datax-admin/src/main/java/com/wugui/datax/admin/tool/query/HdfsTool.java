package com.wugui.datax.admin.tool.query;


import com.wugui.datax.admin.entity.JobDatasource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;
import java.util.concurrent.ExecutionException;


public class HdfsTool {
    private String url;
    private PortTool portTool;

    public HdfsTool(JobDatasource jobDatasource) {
        url=jobDatasource.getJdbcUrl();
    }
    public HdfsTool(String lurl) {
        url=lurl;
    }
    public boolean dataSourceTest(){
//        try {
//            Configuration configuration = new Configuration();
//            // 这里我启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
//            configuration.set("dfs.replication", "1");
//            FileSystem fileSystem = FileSystem.get(new URI(url), configuration,"admin");
//            return true;
//        }catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
        try {
            String lurl=url.replace("hdfs://","");
            String[] hosts=lurl.split(":");
            String host=hosts[0];
            int port=Integer.parseInt(hosts[1]);
            return PortTool.scan(host,port);
        } catch (Exception e) {
            return false;
        }
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        HdfsTool hdfsTool=new HdfsTool("hdfs://172.16.20.72:8020");
        System.out.println(hdfsTool.dataSourceTest());
    }
}
