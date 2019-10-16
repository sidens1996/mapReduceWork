package com.csu.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: KVTextMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 22/09/2019  16:54
 * @Version: 1.0
 **/

public class KVTextMapper extends Mapper<Text,Text,Text, IntWritable> {

    IntWritable v = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        // 1 读取一行

        // 2 封装对象

        // 3 写出
        context.write(key,v);
    }
}
