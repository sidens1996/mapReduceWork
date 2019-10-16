package com.csu.mr.findcommon2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: CommonFriendsReducerStep1
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  21:13
 * @Version: 1.0
 **/

public class CommonFriendsReducerStep1 extends Reducer<Text,Text,Text,Text> {

    String rightResults;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //  右侧是左侧好友进行局部汇总
        rightResults = "";
        for (Text value : values) {

            rightResults += (value.toString() + " ");
        }

        // 粉丝 明星
        context.write(key,new Text(rightResults));
    }
}
