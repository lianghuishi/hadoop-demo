package compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneCompareMapper extends Mapper<LongWritable, Text, PhoneCompareBean, LongWritable> {

    PhoneCompareBean k = new PhoneCompareBean();
    LongWritable v = new LongWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String phone = split[0];
        Long upFlow =  Long.parseLong(split[1]);
        Long downFlow = Long.parseLong(split[2]);
        Long order = Long.parseLong(split[3]);
        k.setPhone(phone);
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setOrder(order);
        v.set(order);
        context.write(k,v);
    }
}
