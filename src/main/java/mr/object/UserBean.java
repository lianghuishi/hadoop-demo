package mr.object;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserBean implements Writable {

    private String name;
    private Double amt;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getAmt() {
        return amt;
    }

    public void setAmt(Double amt) {
        this.amt = amt;
    }

    /**
     * 反序列化方法
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeDouble(amt);
    }

    /**
     *  序列化方法
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.amt = in.readDouble();
    }

    @Override
    public String toString() {
        return name+"\t"+amt;
    }
}
