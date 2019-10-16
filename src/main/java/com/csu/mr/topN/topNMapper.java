package com.csu.mr.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: topNMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 12/10/2019  18:27
 * @Version: 1.0
 **/

public class topNMapper extends Mapper<LongWritable, Text,PhoneBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 读取一行
        String line = value.toString();

        // 2 切割数据
        String words[] = line.split("\t");

        // 3 封装数据
        String phoneNum = words[1];
        Long downFlow = Long.parseLong(words[words.length - 3]);
        Long upFlow = Long.parseLong(words[words.length - 2]);
        Long sumFlow = downFlow + upFlow;
        PhoneBean phoneBean = new PhoneBean(phoneNum, downFlow, upFlow, sumFlow);

        // 4 写出
        context.write(phoneBean,NullWritable.get());
    }
}
