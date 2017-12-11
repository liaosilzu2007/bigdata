package com.lzumetal.bigdata.hdfs.rpc.service;

import com.lzumetal.bigdata.hdfs.rpc.protocol.ClientNameNodeProtocol;

/**
 * 类描述：客户端和NameNode通信的实现
 * 创建人：liaosi
 * 创建时间：2017年12月11日
 */
public class ClientNameNodeImp implements ClientNameNodeProtocol {

    @Override
    public String getMetaData(String path) {
        return path+": 3 - {BLK_1,BLK_2} ....";
    }
}
