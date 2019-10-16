package com.csu.mr.findcommon;

import com.csu.mr.kv.KVTextReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;



/**
 * @ClassName: CommonFriendsDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  15:34
 * @Version: 1.0
 **/

public class CommonFriendsDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息
        Configuration conf = new Configuration();
        // 设置分隔符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,":");

        // 2 获取job对象
        Job job = Job.getInstance(conf);

        // 设置输入类
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 3 设置jar存储路径
        job.setJarByClass(CommonFriendsDriver.class);

        // 4 设置mapper和reducer类
        job.setMapperClass(CommonFriendsMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        // 5 设置mapper输出key和value类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 6 设置最终输出key和value类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 7 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\friends.txt"
                ,"E:\\code_in_IDEA\\wordcount\\output\\output_common_friends"};
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 8 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
