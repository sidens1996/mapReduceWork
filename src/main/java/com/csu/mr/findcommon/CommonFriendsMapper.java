package com.csu.mr.findcommon;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * @ClassName: CommonFriendsMapper
 * @Description: TODO
 * @Author: Achilles
 * @Date: 12/10/2019  22:05
 * @Version: 1.0
 **/

public class CommonFriendsMapper extends Mapper<Text,Text,Text,Text> {

    // 输入 A:B,C,D,F,E,O
    // 要求最终输出 A>B:C,D,E
    // 怎么实现
    // 初步思路：首先对所有的人进行遍历，再对人的好友进行遍历，再找出与下一个人相同的好友
    // 三层循环
    // 如果是mapreduce又该如何实现
    // 1 使用KVTextInputFormat进行读取数据 K A; V B,C,D,E,F,O
    // 2 对V进行切割B C D R F O
    // 3 写出
    // 4 reduce数据
    // A B C D R F O
    // B A D F C Q
    // 换个思路
    // 1 2 相同
    // 然后按照A,B A,C这样的顺序依次遍历，将共同好友放入同一个value，A，B作为key

    TreeMap<String, String> friendsMap = new TreeMap<String, String>();
    ArrayList<String> friendsList = new ArrayList<String>();
    ArrayList<String> myList = new ArrayList<String>();
    int i = 0;
    //hashmap是有序的

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String friends = value.toString().replace(","," ");
        friendsList.add(i,friends);
        myList.add(i,key.toString());

        i++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for(int i = 0; i < friendsList.size()-1; i++){
            for (int j = i + 1;j < friendsList.size();j++){

                StringBuilder firsString = new StringBuilder(myList.get(i) + "<->" + myList.get(j));
                StringBuilder secString = new StringBuilder(friendsList.get(i) + " " + friendsList.get(j));

                friendsMap.put(firsString.toString(),secString.toString());
            }
        }

        for (String string : friendsMap.keySet()) {

            context.write(new Text(string.toString()),
                    new Text(friendsMap.get(string).toString()));
        }
    }
}
