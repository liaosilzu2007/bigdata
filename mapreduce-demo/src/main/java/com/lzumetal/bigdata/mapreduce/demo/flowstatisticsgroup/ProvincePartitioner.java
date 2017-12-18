package com.lzumetal.bigdata.mapreduce.demo.flowstatisticsgroup;

import com.lzumetal.bigdata.mapreduce.demo.flowstattistics.FlowStatisticsBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 类描述：map任务输出按照省份分组
 * 创建人：liaosi
 * 创建时间：2017年12月12日
 *
 * K2  V2  对应的是map输出kv的类型
 */
public class ProvincePartitioner extends Partitioner<Text, FlowStatisticsBean> {

    //Map的value是Integer型
    private static HashMap<String, Integer> proviceMap = new HashMap<>();

    static {
        proviceMap.put("136", 0);
        proviceMap.put("137", 1);
        proviceMap.put("138", 2);
        proviceMap.put("139", 3);
    }


    @Override
    public int getPartition(Text text, FlowStatisticsBean flowStatisticsBean, int i) {
        String province = text.toString().substring(0, 3);
        Integer part = proviceMap.get(province);
        return part == null ? 4 : part;
    }
}
