package com.csu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: WordCountReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 16/09/2019  11:02
 * @Version: 1.0
 **/

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        int sum = 0;
        // 1 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 输出结果
        IntWritable v = new IntWritable(sum);
        context.write(key,v);
        //因为这里每次都已经写出了，所以即使是引用也能得到正确结果

    }
}
