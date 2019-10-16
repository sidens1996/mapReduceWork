package com.csu.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: IndexCombiner
 * @Description: TODO
 * @Author: Achilles
 * @Date: 11/10/2019  17:13
 * @Version: 1.0
 **/

public class IndexCombiner extends Reducer<IndexBean, LongWritable, IndexBean, LongWritable> {

    @Override
    protected void reduce(IndexBean key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        LongWritable v = new LongWritable();
        long sum = 0l;
        for (LongWritable value : values) {

            sum += Integer.parseInt(value.toString());
        }

        v.set(sum);
        context.write(key,v);
    }
}
