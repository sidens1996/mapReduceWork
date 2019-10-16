package com.csu.mr.findcommon2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: CommonFriendsMapperStep2
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  21:48
 * @Version: 1.0
 **/

public class CommonFriendsMapperStep2 extends Mapper<Text,Text,KeyBean,Text> {

    // 思路：读取第一阶段输出结果
    // 第一次输出结果中，key为value中任意两人的共同粉丝
    KeyBean keyBean = new KeyBean();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] rights = value.toString().split(" ");
        for (int i = 0; i < rights.length - 1; i++) {
            for (int j = i; j < rights.length; j++) {

                // 左边比右边小
                keyBean.setFirst(rights[i].compareTo(rights[j]) > 0 ? rights[j] : rights[i]);
                keyBean.setSecond(rights[i].compareTo(rights[j]) > 0 ? rights[i] : rights[j]);
                context.write(keyBean,new Text(key));
            }
        }
    }


}
