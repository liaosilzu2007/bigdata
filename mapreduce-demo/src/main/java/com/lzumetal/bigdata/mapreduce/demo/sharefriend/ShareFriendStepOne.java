package com.lzumetal.bigdata.mapreduce.demo.sharefriend;

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
import org.junit.Test;

import java.io.IOException;

/**
 * 类描述：qq共同好友分析第一个步骤
 * 创建人：liaosi
 * 创建时间：2017年12月14日
 */
public class ShareFriendStepOne {


    static class ShareFriendStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] persons = value.toString().split(":");
            String person = persons[0];
            String[] friends = persons[1].split(",");
            for (String friend : friends) {
                context.write(new Text(friend), new Text(person));
            }
        }

    }


    static class ShareFriendStepOneReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder stringBuilder = new StringBuilder();
            for (Text person : values) {
                stringBuilder.append(person).append(",");
            }
            Text outputValue = new Text();
            if (stringBuilder.length() != 0) {
                String persons = stringBuilder.substring(0, stringBuilder.length() - 1);
                outputValue.set(persons);
            }
            context.write(key, outputValue);
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ShareFriendStepOne.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ShareFriendStepOneMapper.class);
        job.setReducerClass(ShareFriendStepOneReducer.class);

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
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 2; i++) {
            stringBuilder.append(i).append(",");
        }
        System.out.println(stringBuilder.substring(0, stringBuilder.length() - 1));

        String str = "abcde";
        System.out.println(str.substring(2, 3));
    }

}
