package com.csu.mr.findcommon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ClassName: CommonFriendsReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 12/10/2019  22:05
 * @Version: 1.0
 **/

public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {

    ArrayList<String> elements = new ArrayList<String>();
    ArrayList<String> results = new ArrayList<String>();
    String s = new String();
    int x = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 输入 A<->B D F G D F
        // 找出value中重复的元素即为共同好友

        s = "";
        elements.add(x, key.toString());
        // value只有一个
        for (Text value : values) {

            String[] words = value.toString().split(" ");
            // 找出出现两次的元素，加入results中
            for (int i = 0; i < words.length - 1; i++) {
                for (int j = i + 1; j < words.length; j++) {

                    // 两个String比较是否相等不能用==，因为String是引用类型，不是基本数据类型，故他们的比较是
                    // 使用地址和值来比较，而不只是值
                    if (words[i].equals(words[j])) {
                        s += words[i];
                    }
                }
            }
        }
        results.add(x,s);

        x++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (int i = 0; i < elements.size(); i++){

            if (results.get(i) != ""){
                context.write(new Text(elements.get(i)),new Text(results.get(i)));
            }
        }
    }
}


// TODO 试一下视频中的另一种实现思路