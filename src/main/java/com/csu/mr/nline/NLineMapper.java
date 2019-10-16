package com.csu.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: NLineMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 25/09/2019  21:07
 * @Version: 1.0
 **/

public class NLineMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    Text k = new Text();
    LongWritable v = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取1行
        String line = value.toString();
        String[] words = line.split(" ");

        // 2 封装对象
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }

    }
}
