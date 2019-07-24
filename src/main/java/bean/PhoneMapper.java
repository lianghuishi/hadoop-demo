package bean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, Text, PhoneBean> {

    Text k = new Text();
    PhoneBean v = new PhoneBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String phone = split[0];
        Long upFlow =  Long.parseLong(split[1]);
        Long downFlow = Long.parseLong(split[2]);
        k.set(phone);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        context.write(k,v );
    }
}
