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

import java.io.IOException;
import java.util.Arrays;

/**
 * 类描述：qq共同好友分析第二个步骤
 * 创建人：liaosi
 * 创建时间：2017年12月14日
 */
public class ShareFriendStepTwo {

    static class ShareFriendStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            Text friend = new Text(split[0]);

            String[] persons = split[1].split(",");
            Arrays.sort(persons);
            //C A,B,G,N
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text(persons[i] + "-" + persons[j]), friend);
                }
            }
        }
    }

    static class ShareFriendStepTwoReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder stringBuilder = new StringBuilder();
            for (Text value : values) {
                stringBuilder.append(value).append(",");
            }
            Text ouputValue = new Text();
            if (stringBuilder.length() > 0) {
                ouputValue.set(stringBuilder.substring(0, stringBuilder.length() - 1));
            }
            context.write(key, ouputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ShareFriendStepTwo.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ShareFriendStepTwoMapper.class);
        job.setReducerClass(ShareFriendStepTwoReducer.class);

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

    /*@Test
    public void test() {
        String[] strings = {"A", "B", "C", "D"};
        for (int i = 0; i < strings.length - 1; i++) {
            for (int j = i + 1; j < strings.length; j++) {
                System.out.println(strings[i] + "-" + strings[j]);
            }
        }
    }*/
}
