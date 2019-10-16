package com.csu.mr.custominput;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: SequenceFileReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 28/09/2019  19:52
 * @Version: 1.0
 **/

public class SequenceFileReducer extends Reducer<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key,values.iterator().next());
    }
}
