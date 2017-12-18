package com.lzumetal.bigdata.mapreduce.demo.inverseindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 类描述：有大量的文本（文档、网页），需要建立搜索索引
 * 创建人：liaosi
 * 创建时间：2017年12月15日
 */
public class InverseIndexStepOne {


    static class InverseIndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String[] split = value.toString().split(" ");
            for (String word : split) {
                if (StringUtils.isNotBlank(word)) {
                    outputKey.set(word.trim() + "," + fileSplit.getPath().getName());
                    context.write(outputKey, outputValue);
                }
            }
        }

    }

    static class InverseIndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            outputValue.set(count);
            context.write(key, outputValue);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(InverseIndexStepOne.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(InverseIndexStepOneMapper.class);
        job.setReducerClass(InverseIndexStepOneReducer.class);

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
