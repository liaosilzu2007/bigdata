package com.lzumetal.bigdata.mapreduce.demo.flowstattistics;

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
 * 类描述：移动供应商流量统计计算示例
 * 创建人：liaosi
 * 创建时间：2017年12月12日
 */
public class FlowStatistics {

    static class FlowStatisticsMapper extends Mapper<LongWritable, Text, Text, FlowStatisticsBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] datas = line.split("\t");

            FlowStatisticsBean flowStatisticsBean = new FlowStatisticsBean(Long.parseLong(datas[datas.length - 3]), Long.parseLong(datas[datas.length - 2]));
            context.write(new Text(datas[1]), flowStatisticsBean);
        }

    }

    static class FlowStatisticsReducer extends Reducer<Text, FlowStatisticsBean, Text, FlowStatisticsBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowStatisticsBean> values, Context context) throws IOException, InterruptedException {
            long uploadFlowSum = 0;
            long downFlowSum = 0;

            for (FlowStatisticsBean flowStatisticsBean : values) {
                uploadFlowSum += flowStatisticsBean.getUploadFlow();
                downFlowSum += flowStatisticsBean.getDownFlow();
            }

            FlowStatisticsBean flowStatisticsBean = new FlowStatisticsBean(uploadFlowSum, downFlowSum);
            context.write(key, flowStatisticsBean);
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowStatistics.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowStatisticsBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FlowStatisticsMapper.class);
        job.setReducerClass(FlowStatisticsReducer.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
