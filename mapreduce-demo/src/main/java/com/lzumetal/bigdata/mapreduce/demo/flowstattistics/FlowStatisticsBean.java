package com.lzumetal.bigdata.mapreduce.demo.flowstattistics;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类描述：
 * 创建人：liaosi
 * 创建时间：2017年12月12日
 * WritableComparable的泛型表示的是和哪一种类型数据进行比较
 */
public class FlowStatisticsBean implements WritableComparable<FlowStatisticsBean> {

    private long uploadFlow;
    private long downFlow;
    private long sumFlow;

    public FlowStatisticsBean() {
    }

    public FlowStatisticsBean(long uploadFlow, long downFlow) {
        this.uploadFlow = uploadFlow;
        this.downFlow = downFlow;
        this.sumFlow = uploadFlow + downFlow;
    }

    public void set(long uploadFlow, long downFlow) {
        this.uploadFlow = uploadFlow;
        this.downFlow = downFlow;
        this.sumFlow = uploadFlow + downFlow;
    }

    public long getUploadFlow() {
        return uploadFlow;
    }

    public void setUploadFlow(long uploadFlow) {
        this.uploadFlow = uploadFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return uploadFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(uploadFlow);
        dataOutput.writeLong(downFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uploadFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = this.uploadFlow + this.downFlow;
    }

    @Override
    public int compareTo(FlowStatisticsBean bean) {
        long diff = this.sumFlow - bean.getSumFlow();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        } else {
            return 0;
        }
    }
}
