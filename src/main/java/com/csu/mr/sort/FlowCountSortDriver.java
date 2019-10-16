package com.csu.mr.sort;

import com.csu.mr.flowsum.FlowCountMapper;
import com.csu.mr.flowsum.FlowCountReducer;
import org.apache.hadoop.io.Text;
// 不要导错包
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: FlowCountSortDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 08/10/2019  17:09
 * @Version: 1.0
 **/

public class FlowCountSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息
        Configuration conf = new Configuration();

        // 2 获取job对象
        Job job = Job.getInstance(conf);

        // 3 设置jar存储路径
        job.setJarByClass(FlowCountSortDriver.class);

        // 4 设置mapper和reducer类
        // 注意类名不是之前写的非sort
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 5 设置mapper输出的key和value
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 6 设置输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置reducetask数量，和分区数目相对应
        job.setNumReduceTasks(5);
        // 设置分区类
        job.setPartitionerClass(ProvincePartitioner.class);

        // 7 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\phone_data.txt",
                "E:\\code_in_IDEA\\wordcount\\output\\output_sort_partition"};
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
