package com.csu.mr.index;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: IndexGroupingComparator
 * @Description: TODO
 * @Author: Achilles
 * @Date: 11/10/2019  16:43
 * @Version: 1.0
 **/

public class IndexGroupingComparator extends WritableComparator {

    // 辅助排序一定要写这个构造函数，否则会报空指针异常，且调试不易找出错误
    public IndexGroupingComparator() {
        super(IndexBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        IndexBean aBean = (IndexBean)a;
        IndexBean bBean = (IndexBean)b;

        // 开始比较
        int result = 0;
        if (aBean.getWord().compareTo(bBean.getWord()) > 0) {
            result = 1;
        } else if (aBean.getWord().compareTo(bBean.getWord()) < 0) {
            result = -1;
        }else{
//            if (aBean.getFileName().compareTo(bBean.getFileName()) > 0) {
//                result = 1;
//            } else if (aBean.getFileName().compareTo(bBean.getFileName()) < 0) {
//                result = -1;
//            }else {
                result = 0;// combiner的结果，如果word相同进入同一个reduce
//            }
        }

        return result;
    }
}
