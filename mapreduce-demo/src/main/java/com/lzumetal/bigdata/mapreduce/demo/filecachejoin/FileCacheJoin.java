package com.lzumetal.bigdata.mapreduce.demo.filecachejoin;

import com.lzumetal.bigdata.mapreduce.demo.orderjoin.OrderInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * 类描述：利用Hadoop中的DistributedCache组件，map task可以读取一个放在缓存中的文件，
 * 这样可以在map端进行join，而不再进行reduce，解决数据倾斜的问题。
 * （数据倾斜：比如订单表和产品表，订单表中存在大量订单是同一个产品的，
 * 正常的情况下会分组后把这大量订单给某一个reduce task，导致reduce task的任务量不均匀）
 * 创建人：liaosi
 * 创建时间：2017年12月15日
 */
public class FileCacheJoin {


    static class FileCacheJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        Map<String, OrderInfo> productMap = new HashMap<>();
        Text outputKey = new Text();

        /*
         * 通过阅读Mapper的源码，在执行map任务时，实际上是调用的Mapper中的run方法，
         * 在run方法中是先执行setup方法，然后循环调用map方法，所以我们可以在setup方法中做一些初始化工作
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("product.file")));

//            Path[] caches = DistributedCache.getLocalCacheFiles(context
//                    .getConfiguration());
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(caches[0].toString())));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split("\t");
                OrderInfo orderInfo = new OrderInfo();
                orderInfo.setProductName(split[1]);
                orderInfo.setCatogeryId(Integer.parseInt(split[2]));
                orderInfo.setProductPrice(Integer.parseInt(split[3]));
                productMap.put(split[0], orderInfo);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            OrderInfo orderInfo = productMap.get(line.split("\t")[2]);
            if (orderInfo != null) {
                outputKey.set(line + "\t" + orderInfo.getProductName() + "\t" + orderInfo.getCatogeryId() + "\t" + orderInfo.getProductPrice());
            } else {
                outputKey.set(line);
            }
            context.write(outputKey, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FileCacheJoin.class);

        job.setMapperClass(FileCacheJoinMapper.class);

        //因为没有reduce，mapOutput不用设置，直接把map的输出作为Output输出即可。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //缓存一个文件到所有的map task运行节点
        /*job.addArchiveToClassPath(args);*/    //缓存jar包到map task运行节点的classpath下
        /*job.addCacheArchive(uri);*/   //缓存压缩包文件到map task运行节点目录下
        /*job.addFileToClassPath(file);*/   //缓存文件到map task运行节点的classpath下
        job.addCacheFile(new URI("file:///home/hadoop/product.file"));  //缓存文件到map task运行节点的目录下，该文件在本地或者hdfs中都可以。
        //job.addCacheFile(new URI("hdfs://server01:9000/cache/product.file"));
        /*
         * 在这个示例中，使用hdfs上的文件作为分布式缓存时，是没有问题的。
         * 但是使用本地文件时，报异常：
         * java.io.FileNotFoundException: File file:/home/hadoop/product.file does not exist
         */

        //map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

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

}
