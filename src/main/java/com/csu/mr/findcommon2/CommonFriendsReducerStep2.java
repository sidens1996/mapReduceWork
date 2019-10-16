package com.csu.mr.findcommon2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: CommonFriendsReducerStep2
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  21:49
 * @Version: 1.0
 **/

public class CommonFriendsReducerStep2 extends Reducer<KeyBean, Text, KeyBean, Text> {

    String rightResults;

    @Override
    protected void reduce(KeyBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        rightResults = "";
        // 汇总map阶段的数据
        for (Text value : values) {
            rightResults += value;
        }
        if (!key.getFirst().equals(key.getSecond()))
            context.write(key, new Text(rightResults));
    }
}
