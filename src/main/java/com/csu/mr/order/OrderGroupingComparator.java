package com.csu.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: OrderGroupingComparator
 * @Description: TODO
 * @Author: Achilles
 * @Date: 09/10/2019  15:37
 * @Version: 1.0
 **/

public class OrderGroupingComparator extends WritableComparator {

    // 如果不要这个构造函数，输出将为空，报空指针异常
    public OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        // 要求只要id相同就认为是相同的key，送入同一个reduce方法
        int result = 0;

        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        if (aBean.getOrder_id() > bBean.getOrder_id()) {
            result = 1;
        } else if (aBean.getOrder_id() < bBean.getOrder_id()) {
            result = -1;
        }else{
            result = 0;
        }

        return result;
    }
}
