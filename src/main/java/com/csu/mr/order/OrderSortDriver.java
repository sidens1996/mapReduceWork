package com.csu.mr.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: OrderSortDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 09/10/2019  15:01
 * @Version: 1.0
 **/

public class OrderSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar存储路径
        job.setJarByClass(OrderSortDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(OrderSortMapper.class);
        job.setReducerClass(OrderSortReducer.class);

        // 4 设置mapper输出的key和value
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出的key和value
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置分组排序
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        // 6 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\GroupingComparator.txt",
                "E:\\code_in_IDEA\\wordcount\\output\\output_GroupingComparator"};
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 提交job
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);

    }
}
