package com.csu.mr.findcommon2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: CommonFriendsMapperStep1
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  19:36
 * @Version: 1.0
 **/

// 输入
// A:B,C,D,F,E,O
// B:A,C,E,K

// 思路：首先将数据由主动格式转换为被动格式，即右侧是左侧好友转换为左侧是右侧好友
// 转换为人-人对形式，且右侧是左侧的好友

// 或者将这个问题转化为求共同粉丝的问题，左边是明星，右边是粉丝，一个明显可能有几百万粉丝，在一个map里面
// 遍历不现实，转换为粉丝关注的明星更好，一个粉丝关注的明星并不多，因此一个map处理的内容不多，但map任务多
public class CommonFriendsMapperStep1 extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割数据
        String[] words = line.split(":");

        // 3 封装数据
        String right = words[0];
        String[] lefts = words[1].split(",");

        // 4 写出
        for (String left : lefts) {

            // 粉丝 明星
            context.write(new Text(left),new Text(right));
        }
    }
}
