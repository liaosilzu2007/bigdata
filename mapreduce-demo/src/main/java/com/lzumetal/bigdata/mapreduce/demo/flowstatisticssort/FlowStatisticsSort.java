package com.lzumetal.bigdata.mapreduce.demo.flowstatisticssort;

import com.lzumetal.bigdata.mapreduce.demo.flowstattistics.FlowStatisticsBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 类描述：对流量进行统计并将结果根据总流量倒序排序
 * 创建人：liaosi
 * 创建时间：2017年12月13日
 */
public class FlowStatisticsSort {

    static class FlowStatisticsSortMapper extends Mapper<LongWritable, Text, FlowStatisticsBean, Text> {

        private FlowStatisticsBean flowStatisticsBean = new FlowStatisticsBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            flowStatisticsBean.set(Long.parseLong(datas[1]), Long.parseLong(datas[2]));

            context.write(flowStatisticsBean, new Text(datas[0]));
        }
    }


    static class FlowStatisticsSortReducer extends Reducer<FlowStatisticsBean, Text, Text, FlowStatisticsBean> {

        @Override
        protected void reduce(FlowStatisticsBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowStatisticsSort.class);

        job.setMapOutputKeyClass(FlowStatisticsBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowStatisticsBean.class);

        job.setMapperClass(FlowStatisticsSort.FlowStatisticsSortMapper.class);
        job.setReducerClass(FlowStatisticsSort.FlowStatisticsSortReducer.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
    }

}
