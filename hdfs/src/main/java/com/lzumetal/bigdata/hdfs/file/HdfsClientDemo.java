package com.lzumetal.bigdata.hdfs.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * 类描述：hadoop客户端测试类
 * 创建人：liaosi
 * 创建时间：2017年12月10日
 */
public class HdfsClientDemo {

    private FileSystem fileSystem;
    private static Path path = new Path("/test");

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        /**
         * 客户端去操作hdfs时，是有一个用户身份的
         * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
         *
         * 也可以在构造客户端fs对象时，通过参数传递进去
         */
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://server01:9000");
//        fileSystem = FileSystem.get(configuration);
        fileSystem = FileSystem.get(new URI("hdfs://server01:9000"), new Configuration(), "hadoop");

    }

    /**
     * 将本地文件上传至hadoop集群的HDFS文件系统
     * @throws IOException
     */
    @Test
    public void uploadTest() throws IOException {

        if (!fileSystem.exists(path)) {
            //目录可以多级创建
            fileSystem.mkdirs(path);
        }

        fileSystem.copyFromLocalFile(new Path("D:/TomcatLogs/logs/duboo-learn.log"), path);
        fileSystem.close();
    }

    /**
     * 删除HDFS文件系统的文件或文件夹
     * @throws IOException
     */
    @Test
    public void deleteFile() throws IOException {
        Path path = new Path("/test/duboo-learn.log");
        if (fileSystem.exists(path)) {
            System.out.println("删除文件...");
            //fileSystem.delete(new Path("/test"), true);   //true表示是否递归删除
            fileSystem.deleteOnExit(new Path("/test/duboo-learn.log"));
        }
        fileSystem.close();
    }

    /**
     * 从HDFS文件系统复制文件到本地
     * @throws IOException
     */
    @Test
    public void downloadTest() throws IOException {
        fileSystem.copyToLocalFile(new Path("/test/duboo-learn.log"), new Path("d:/"));
        fileSystem.close();
    }

    /**
     * 递归列出指定目录下所有子目录中的文件
     * @throws IOException
     */
    @Test
    public void listFileTest() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);//true表示是否递归
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            System.out.println("path ----> " + fileStatus.getPath());
            System.out.println("owner ----> " + fileStatus.getOwner());
            System.out.println("modifyTime ----> " + new Date(fileStatus.getModificationTime()));
            System.out.println("permission ----> " + fileStatus.getPermission());
            System.out.println("==============================");
        }
        fileSystem.close();
    }


    @Test
    public void FileTest() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath().getName() + " ----> " + fileStatus.isFile());
        }
    }



}
