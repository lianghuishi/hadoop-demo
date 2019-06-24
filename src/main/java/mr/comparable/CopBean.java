package mr.comparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CopBean implements WritableComparable<CopBean> {

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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeDouble(amt);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.amt = dataInput.readDouble();
    }

    @Override
    public int compareTo(CopBean obj) {
        return this.amt-obj.getAmt() > 0 ? 1:-1;
    }

    @Override
    public String toString() {
        return name+"\t"+amt;
    }


}
