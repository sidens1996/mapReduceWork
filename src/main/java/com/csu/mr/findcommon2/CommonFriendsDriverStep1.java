package com.csu.mr.findcommon2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: CommonFriendsDriverStep1
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  21:35
 * @Version: 1.0
 **/

public class CommonFriendsDriverStep1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息
        Configuration conf = new Configuration();

        // 2 获取job对象
        Job job = Job.getInstance(conf);

        // 3设置jar存储路径
        job.setJarByClass(CommonFriendsDriverStep1.class);

        // 4 设置mapper和reducer类
        job.setMapperClass(CommonFriendsMapperStep1.class);
        job.setReducerClass(CommonFriendsReducerStep1.class);

        // 5 设置mapper输出的key和value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 6 设置最终输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 7 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\friends.txt"
                ,"E:\\code_in_IDEA\\wordcount\\output\\output_common_friends_step1"};
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 8 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
