package com.csu.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @ClassName: WordCountDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 16/09/2019  14:16
 * @Version: 1.0
 **/

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        job.setNumReduceTasks(2);

        // 3 关联map和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置combiner组件
        job.setCombinerClass(WordCountReducer.class);

        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\input.txt",
                "E:\\code_in_IDEA\\wordcount\\output\\output_combiner"};

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 提交job
        //job.commit();
        boolean result = job.waitForCompletion(true);//true打印运行信息
        System.exit(result ? 0 : 1);
    }
}
