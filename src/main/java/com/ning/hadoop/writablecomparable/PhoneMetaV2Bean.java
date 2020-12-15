package com.ning.hadoop.writablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ClassName: PhoneMetaBean
 * Description:
 * date: 2020/12/10 16:34
 *
 * @author ningjianjian
 */
public class PhoneMetaV2Bean implements WritableComparable<PhoneMetaV2Bean> {

    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public PhoneMetaV2Bean() {
        super();
    }

    public PhoneMetaV2Bean(Long upFlow, Long downFlow, Long sumFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return  "\t" + upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    @Override
    public int compareTo(PhoneMetaV2Bean o) {
        return Long.compare(o.getSumFlow(), this.getSumFlow());
    }
}
