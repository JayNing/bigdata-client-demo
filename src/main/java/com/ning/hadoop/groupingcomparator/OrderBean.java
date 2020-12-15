package com.ning.hadoop.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ClassName: OrderBean
 * Description:
 * date: 2020/12/15 17:15
 *
 * @author ningjianjian
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private Double price;
    private String id;

    public OrderBean() {
        super();
    }

    public OrderBean(double price, String id) {
        super();
        this.price = price;
        this.id = id;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return id + "\t" + price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //先根据id正序排序
        int compare = id.compareTo(o.getId());
        if (compare == 0){
            //再根据价格倒序排序
            return Double.compare(o.getPrice(), this.price);
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }
}
