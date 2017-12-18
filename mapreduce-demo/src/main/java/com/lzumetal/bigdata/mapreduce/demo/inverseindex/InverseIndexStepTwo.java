package com.lzumetal.bigdata.mapreduce.demo.inverseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
 * 类描述：最后输出格式如下：
 * hello	c.txt-->2	b.txt-->2	a.txt-->3
 * jerry	c.txt-->1	b.txt-->3	a.txt-->1
 * tom	    c.txt-->1	b.txt-->1	a.txt-->2
 * 创建人：liaosi
 * 创建时间：2017年12月15日
 */
public class InverseIndexStepTwo {

    static class InverseIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text ouputKey = new Text();
        Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            ouputKey.set(split[0].split(",")[0]);
            outputValue.set(split[0].split(",")[1] + "-->" + split[1]);
            context.write(ouputKey, outputValue);
        }
    }

    static class InverseIndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {

        Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text value : values) {
                stringBuilder.append(value.toString()).append("\t");
            }
            outputValue.set(stringBuilder.toString());
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(InverseIndexStepTwo.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(InverseIndexStepTwoMapper.class);
        job.setReducerClass(InverseIndexStepTwoReducer.class);

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
