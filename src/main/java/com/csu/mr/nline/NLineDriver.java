package com.csu.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: NLineDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 25/09/2019  21:30
 * @Version: 1.0
 **/

public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取Job对象
        Configuration conf = new Configuration() ;
        Job job = Job.getInstance(conf);

        // 设置每个切片InputSolit中划分出5条记录,
        // 则切片总数等于 总行数整除5 + 1
        NLineInputFormat.setNumLinesPerSplit(job,5);
        // 设置输入格式
        job.setInputFormatClass(NLineInputFormat.class);

        // 2 设置jar存储路径
        job.setJarByClass(NLineDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 4 设置map阶段输出的key和value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 5 设置最终输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 6 设置输入输出路径
        args = new String[] {"E:\\code_in_IDEA\\wordcount\\input\\input_nl.txt",
                "E:\\code_in_IDEA\\wordcount\\output_nl"};
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}

