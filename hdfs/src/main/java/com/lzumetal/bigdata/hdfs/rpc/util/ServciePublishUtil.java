package com.lzumetal.bigdata.hdfs.rpc.util;

import com.lzumetal.bigdata.hdfs.rpc.protocol.ClientNameNodeProtocol;
import com.lzumetal.bigdata.hdfs.rpc.service.ClientNameNodeImp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * 类描述：用来发布服务的工具类
 * 创建人：liaosi
 * 创建时间：2017年12月12日
 */
public class ServciePublishUtil {

    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        RPC.Server server = builder.setProtocol(ClientNameNodeProtocol.class)
                .setBindAddress("localhost")
                .setPort(8888)
                .setInstance(new ClientNameNodeImp())
                .build();

        //启动服务端
        server.start();
    }
}
