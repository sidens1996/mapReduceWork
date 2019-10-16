package com.csu.mr.index;

import javafx.scene.text.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: IndexDriver
 * @Description: TODO
 * @Author: Achilles
 * @Date: 11/10/2019  18:42
 * @Version: 1.0
 **/

public class IndexDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息
        Configuration conf = new Configuration();

        // 2 获取job对象
        Job job = Job.getInstance(conf);

        // 3 设置jar存储路径
        job.setJarByClass(IndexDriver.class);

        // 4 设置mapper和reducer类
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        // 5 设置mapper输出的1key和valul类
        job.setMapOutputKeyClass(IndexBean.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 6 设置最终输出的key和value类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 9 设置combiner类
        job.setCombinerClass(IndexCombiner.class);

        // 10 设置分组排序（辅助排序）类
        job.setGroupingComparatorClass(IndexGroupingComparator.class);

        // 7 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\input\\input_index"
                ,"E:\\code_in_IDEA\\wordcount\\output\\output_index"};
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //8 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
