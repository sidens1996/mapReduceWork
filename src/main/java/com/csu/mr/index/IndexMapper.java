package com.csu.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName: IndexMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 11/10/2019  16:51
 * @Version: 1.0
 **/

public class IndexMapper extends Mapper<LongWritable, Text, IndexBean,LongWritable> {

    // 输入输出示例
    // atguigu pingping
    // word filename num

    IndexBean k = new IndexBean();
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 读取一行
        String line = value.toString();

        // 2 获取文件名
        InputSplit inputSplit = context.getInputSplit();
        String fileName = ((FileSplit) inputSplit).getPath().getName();

        // 3 切割数据
        String[] words = line.split(" ");

        // 4 封装对象
        for (String word : words) {
            k.setWord(word);
            k.setFileName(fileName);
            context.write(k,v);
        }
    }
}
