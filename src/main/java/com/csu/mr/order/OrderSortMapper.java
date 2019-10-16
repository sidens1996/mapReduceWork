package com.csu.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: OrderSortMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 09/10/2019  14:33
 * @Version: 1.0
 **/

public class OrderSortMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 0000001	Pdt_01	222.8
        // 1 读取一行
        String line = value.toString();

        // 2 切割数据
        String words[] = line.split("\t");

        // 3 封装数据
        k.setOrder_id(Integer.parseInt(words[0]));
        k.setPrice(Double.parseDouble(words[2]));

        // 4 写出
        context.write(k,NullWritable.get());
    }
}
