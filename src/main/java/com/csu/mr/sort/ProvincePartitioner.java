package com.csu.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: ProvincePartitioner
 * @Description: TODO
 * @Author: Achilles
 * @Date: 09/10/2019  10:36
 * @Version: 1.0
 **/

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean bean, Text text, int i) {

        // 进行分区,一共5个分区
        int result = 4;

        // 分区核心逻辑
        String prePhoneNum = text.toString().substring(0,3);
        if ("136".equals(prePhoneNum)) {
            result = 0;
        } else if ("137".equals(prePhoneNum)) {
            result = 1;
        } else if ("138".equals(prePhoneNum)) {
            result = 2;
        } else if ("139".equals(prePhoneNum)) {
            result = 3;
        }
        return result;
    }
}
