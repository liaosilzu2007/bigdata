package com.lzumetal.bigdata.hdfs.rpc.client;

import com.lzumetal.bigdata.hdfs.rpc.protocol.ClientNameNodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 类描述：模拟一个HDFS客户端
 * 创建人：liaosi
 * 创建时间：2017年12月11日
 */
public class MyHdfsClient {

    public static void main(String[] args) throws IOException {
        ClientNameNodeProtocol proxy = RPC.getProxy(ClientNameNodeProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
        String metaData = proxy.getMetaData("/abc.txt");
        System.out.println(metaData);
    }
}
