package com.csu.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: KVTextReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 25/09/2019  16:26
 * @Version: 1.0
 **/

public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        // 1 汇总
        for (IntWritable value : values) {
            sum += Integer.parseInt(value.toString());
        }
        v.set(sum);

        // 2 写出
        context.write(key,v);
    }
}
