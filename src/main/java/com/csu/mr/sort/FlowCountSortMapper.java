package com.csu.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: FlowCountSortMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 08/10/2019  16:56
 * @Version: 1.0
 **/

public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 读取一行
        String line = value.toString();

        // 2 切割字段
        String[] words = line.split("\t");

        // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        // 3 封装对象
        long upFlow = Long.parseLong(words[words.length - 2]);
        long downFlow = Long.parseLong(words[words.length - 1]);
        long sumFlow = upFlow + downFlow;

        String phoneNum = words[1];
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);
        v.set(phoneNum);

        // 4 写出
        context.write(k,v);
    }
}
