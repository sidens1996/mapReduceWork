package com.csu.mr.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: topNDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 12/10/2019  18:54
 * @Version: 1.0
 **/

public class topNDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息
        Configuration conf = new Configuration();

        // 2 获取job对象
        Job job = Job.getInstance(conf);

        // 3 设置jar存储路径
        job.setJarByClass(topNDriver.class);

        // 4 设置mapper和reduer
        job.setMapperClass(topNMapper.class);
        job.setReducerClass(topNReducer.class);

        // 5 设置mapper输出的keyh和value类
        job.setMapOutputKeyClass(PhoneBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 6 设置最终输出的key和value类
        job.setOutputKeyClass(PhoneBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 7 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\phone_data.txt"
                , "E:\\code_in_IDEA\\wordcount\\output\\output_topN"};
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 8 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
