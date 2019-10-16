package com.csu.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: ProvincePartitioner
 * @Description: TODO
 * @Author: Achilles
 * @Date: 07/10/2019  21:32
 * @Version: 1.0
 **/

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // key是手机号
        // value是流量信息

        // 设置默认5个分区
        int partition = 4;

        // 获取手机号前三位
        // partition只能从0开始写
        String prePhoneNum = text.toString().substring(0, 3);
        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
