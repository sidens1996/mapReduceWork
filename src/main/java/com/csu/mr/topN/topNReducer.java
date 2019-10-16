package com.csu.mr.topN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @ClassName: topNReducer
 * @Description: TODO
 * @Author: Achilles
 * @Date: 12/10/2019  18:35
 * @Version: 1.0
 **/

public class topNReducer extends Reducer<PhoneBean, NullWritable, PhoneBean, NullWritable> {

    // treemap中不允许重复元素，queue中允许
    LinkedList<PhoneBean> linkedList = new LinkedList<PhoneBean>();

    @Override
    protected void reduce(PhoneBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        PhoneBean bean = new PhoneBean();
        bean.setPhoneNum(key.phoneNum);
        bean.setUpFlow(key.upFlow);
        bean.setDownFlow(key.downFlow);
        bean.setSumFlow(key.sumFlow);

        // 这里的key是引用，如果只把引用放入queue中，那么queue将会始终是最后放入的一个引用的值
        linkedList.add(bean);

        if (linkedList.size() > 10){
                linkedList.removeLast();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (PhoneBean phoneBean : linkedList) {

            context.write(phoneBean,NullWritable.get());
        }
    }
}
