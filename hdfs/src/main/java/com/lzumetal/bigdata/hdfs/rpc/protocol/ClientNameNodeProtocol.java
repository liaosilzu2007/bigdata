package com.lzumetal.bigdata.hdfs.rpc.protocol;

/**
 * 类描述：客户端和NameNode通信的接口
 * 创建人：liaosi
 * 创建时间：2017年12月11日
 */
public interface ClientNameNodeProtocol {

    static final long versionID = 1L;

    public abstract String getMetaData(String path);

}
