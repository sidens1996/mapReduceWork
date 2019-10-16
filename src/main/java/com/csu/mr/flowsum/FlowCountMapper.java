package com.csu.mr.flowsum;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: FlowCountMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 22/09/2019  13:23
 * @Version: 1.0
 **/

public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 读取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

//        7 	13560436666	120.196.100.99	1116	954	200
        // 3 封装对象
        String phone = fields[1];
        long upFlow = Long.parseLong(fields[fields.length-1]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        k.set(phone);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        v.setSumFlow(upFlow + downFlow);

        // 4 写出
        context.write(k,v);
    }
}
