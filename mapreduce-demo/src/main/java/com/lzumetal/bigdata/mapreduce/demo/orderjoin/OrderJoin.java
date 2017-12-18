package com.lzumetal.bigdata.mapreduce.demo.orderjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 类描述：将订单记录和商品记录两张表信息合并（hadoop中的join）
 * 创建人：liaosi
 * 创建时间：2017年12月13日
 */
public class OrderJoin {

    static class OrderJoinMapper extends Mapper<LongWritable, Text, Text, OrderInfo> {

        private OrderInfo orderInfo = new OrderInfo();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String pathName = inputSplit.getPath().getName();
            String outputKey = "";
            if (pathName.contains("product")) {
                outputKey = datas[0];
                orderInfo.set(0, "", outputKey, 0, datas[1], Integer.parseInt(datas[2]), Integer.parseInt(datas[3]), "1");
            } else if (pathName.contains("order")) {
                outputKey = datas[2];
                orderInfo.set(Long.parseLong(datas[0]), datas[1], outputKey, Integer.parseInt(datas[3]), "", 0, 0, "0");
            }
            context.write(new Text(outputKey), orderInfo);
        }


    }

    static class JoinOrderReducer extends Reducer<Text, OrderInfo, OrderInfo, NullWritable> {


        @Override
        protected void reduce(Text key, Iterable<OrderInfo> values, Context context) throws IOException, InterruptedException {

            OrderInfo productInfo = new OrderInfo();
            List<OrderInfo> orderInfos = new ArrayList<>();
            try {
                for (OrderInfo value : values) {
                    if ("1".equals(value.getBeanType())) {
                        BeanUtils.copyProperties(productInfo, value);
                    } else {
                        OrderInfo orderInfo = new OrderInfo();
                        BeanUtils.copyProperties(orderInfo, value);
                        orderInfos.add(orderInfo);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            for (OrderInfo orderInfo : orderInfos) {
                orderInfo.setProductName(productInfo.getProductName());
                orderInfo.setProductPrice(productInfo.getProductPrice());
                orderInfo.setCatogeryId(productInfo.getCatogeryId());

                context.write(orderInfo, NullWritable.get());
            }


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderJoin.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderInfo.class);

        job.setOutputKeyClass(OrderInfo.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(OrderJoin.OrderJoinMapper.class);
        job.setReducerClass(OrderJoin.JoinOrderReducer.class);

        FileInputFormat.setInputPaths(job, args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

    @Test
    public void test() {
        String name = "order.file";
        System.out.println(name.contains("order"));
    }
}
