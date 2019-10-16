package com.csu.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: OrderBean
 * @Description: TODO
 * @Author: Achilles
 * @Date: 09/10/2019  14:07
 * @Version: 1.0
 **/

public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;   //订单id
    private double price;   //订单价格

    public OrderBean() {
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }

    // 订单升序排列，订单价格降序排列
    @Override
    public int compareTo(OrderBean bean) {

        int result = 0;

        if (order_id > bean.getOrder_id()) {
            result = 1;
        } else if (order_id < bean.getOrder_id()) {
            result = -1;
        }else{
            if (price > bean.getPrice()) {
                result = -1;
            } else if (price < bean.getPrice()) {
                result = 1;
            } else {
                result = 0;
            }
        }
        return result;
    }

    // 序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    // 反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {

        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }
}
