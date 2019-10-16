package com.csu.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: OrderSortReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 09/10/2019  14:51
 * @Version: 1.0
 **/

public class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

//        // 分组排序后送入的key输出第一个最高的价格
//        context.write(key,NullWritable.get());

        int count = 0;
        // 输出最高的两个价格
        for (NullWritable value : values) {
            count++;
            if (count >= 3)
                break;
            context.write(key,NullWritable.get());
        }

        }
    }
