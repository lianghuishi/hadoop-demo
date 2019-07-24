package compare;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/// 1 实现writable接口
public class PhoneCompareBean implements WritableComparable<PhoneCompareBean> {

    private String phone;
    private long upFlow;
    private long downFlow;
    private long order;

    //2  反序列化时，需要反射 调用空参构造函数，所以必须有
    public PhoneCompareBean() {
        super();
    }

    public PhoneCompareBean(String phone, long upFlow, long downFlow, long order) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.order = order;
    }

    //3  写序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(order);
    }

    //4 反序列化方法
    //5 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.order = in.readLong();
    }

    //反序-1，正序1
    @Override
    public int compareTo(PhoneCompareBean bean) {
        return order>bean.getOrder()?-1:1;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getOrder() {
        return order;
    }

    public void setOrder(long order) {
        this.order = order;
    }

    @Override
    public String toString() {
        return "PhoneCompareBean{" +
                "phone='" + phone + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", order=" + order +
                '}';
    }
}
