package com.csu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: WordCountMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 16/09/2019  11:02
 * @Version: 1.0
 **/

//map阶段
//KEYIN 输入的数据的key
//VALUEIN 输入数据的value
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //super.map(key, value, context);

        Text k = new Text();
        IntWritable v = new IntWritable(1);
        //csu csu
        // 1 获取一行
        String line = value.toString();

        // 2 切割单词
        String[] words = line.split(" ");

        // 3 循环写出
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
