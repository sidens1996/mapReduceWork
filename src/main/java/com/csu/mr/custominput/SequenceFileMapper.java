package com.csu.mr.custominput;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: SequenceFileMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 28/09/2019  16:42
 * @Version: 1.0
 **/

public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        context.write(key,value);
    }
}
