package com.csu.mr.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: NLineReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 25/09/2019  21:24
 * @Version: 1.0
 **/

public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable v = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        // 1 循环计数
        for (LongWritable value : values) {

            sum += Long.parseLong(value.toString());
        }
        v.set(sum);

        // 2 写出
        context.write(key,v);
    }
}