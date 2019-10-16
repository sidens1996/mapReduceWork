package com.csu.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: IndexReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 11/10/2019  17:04
 * @Version: 1.0
 **/

// 输入 （atguigu c.txt) 2,combine后的结果
// (atguigu b.txt) 3
// 期望输出 atguigu	c.txt-->2	b.txt-->2	a.txt-->3
public class IndexReducer extends Reducer<IndexBean, LongWritable, Text,Text> {

    String word = "";
    String detail = "";
    String sign = "-->";
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void reduce(IndexBean key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        word = key.getWord();
        for (LongWritable value : values) {
            String fileName = key.getFileName();
            String count = value.toString();
            detail += fileName + sign + count + "\t";
        }
        k.set(word);
        v.set(detail);

        //  用完清空
        word = "";
        detail = "";

        context.write(k,v);
    }
}


