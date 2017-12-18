package com.lzumetal.bigdata.mapreduce.demo.orderjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类描述：订单类model
 * 创建人：liaosi
 * 创建时间：2017年12月13日
 */
public class OrderInfo implements Writable {

    private long orderId;
    private String date;
    private String productId;
    private int amount;
    private String productName;
    private int catogeryId;
    private int productPrice;

    //"0"表示订单，"1"表示商品
    private String BeanType;

    public OrderInfo() {
    }

    public void set(long orderId, String date, String productId, int amount, String productName, int catogeryId, int productPrice, String BeanType) {
        this.orderId = orderId;
        this.date = date;
        this.productId = productId;
        this.amount = amount;
        this.productName = productName;
        this.catogeryId = catogeryId;
        this.productPrice = productPrice;
        this.BeanType = BeanType;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getCatogeryId() {
        return catogeryId;
    }

    public void setCatogeryId(int catogeryId) {
        this.catogeryId = catogeryId;
    }

    public int getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
    }

    public String getBeanType() {
        return BeanType;
    }

    public void setBeanType(String beanType) {
        BeanType = beanType;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(orderId);
        output.writeUTF(date);
        output.writeUTF(productId);
        output.writeInt(amount);
        output.writeUTF(productName);
        output.writeInt(catogeryId);
        output.writeInt(productPrice);
        output.writeUTF(BeanType);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readLong();
        this.date = input.readUTF();
        this.productId = input.readUTF();
        this.amount = input.readInt();
        this.productName = input.readUTF();
        this.catogeryId = input.readInt();
        this.productPrice = input.readInt();
        this.BeanType = input.readUTF();
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "orderId=" + orderId +
                ", date='" + date + '\'' +
                ", productId='" + productId + '\'' +
                ", amount=" + amount +
                ", productName='" + productName + '\'' +
                ", catogeryId=" + catogeryId +
                ", productPrice=" + productPrice +
                ", BeanType='" + BeanType + '\'' +
                '}';
    }
}
