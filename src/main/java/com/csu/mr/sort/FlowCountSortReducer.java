package com.csu.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: FlowCountSortReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 08/10/2019  17:07
 * @Version: 1.0
 **/

public class FlowCountSortReducer extends Reducer<FlowBean, Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value,key);
        }
    }
}
