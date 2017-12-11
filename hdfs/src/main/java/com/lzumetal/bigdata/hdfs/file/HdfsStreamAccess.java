package com.lzumetal.bigdata.hdfs.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 类描述：用流的方式来操作hdfs上的文件，可以实现读取指定偏移量范围的数据
 * 创建人：liaosi
 * 创建时间：2017年12月11日
 */
public class HdfsStreamAccess {

    private FileSystem fileSystem;


    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        fileSystem = FileSystem.get(new URI("hdfs://server01:9000"), conf, "hadoop");
    }


    //用流的方式上传文件
    @Test
    public void uploadTest() throws IOException {
        //true如果该文件已存在，则覆盖原来的文件
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/abc.txt"), true);
        FileInputStream fileInputStream = new FileInputStream("d:/tomcat.txt");

        IOUtils.copy(fileInputStream, fsDataOutputStream);

    }


    //用流的方式下载文件
    @Test
    public void downloadTest() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/abc.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("d:/tomcat_copy.txt");

        IOUtils.copy(fsDataInputStream, fileOutputStream);

    }


    /**
     * 从hdfs文件系统拿取指定大小，指定偏移量的文件
     * 获取指定大小的文件，这在进行MapReduce运算时非常有用，因为我们会在集群的每台服务器上进行MapReduce运算，
     * 每台机器上都取一定大小的文件取处理。
     * @throws IOException
     */
    @Test
    public void rangeDownloadTest() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/abc.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("d:/tomcat_copy.txt");

        IOUtils.copyLarge(fsDataInputStream, fileOutputStream, 4, 6);
    }

}
