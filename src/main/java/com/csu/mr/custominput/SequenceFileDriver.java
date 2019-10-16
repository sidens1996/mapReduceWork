package com.csu.mr.custominput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: SequenceFileDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 28/09/2019  20:11
 * @Version: 1.0
 **/

public class SequenceFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar存储路径
        job.setJarByClass(SequenceFileDriver.class);

        // 7 设置输入的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 8 设置输出的OutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 3 设置map和reduve类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 4 设置map输出的key和value类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 5 设置最终输出的key和value类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input",
                "E:\\code_in_IDEA\\wordcount\\output_wholefile"};
        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
