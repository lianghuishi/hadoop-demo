package bean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReduce extends Reducer<Text, PhoneBean, Text, PhoneBean> {

    @Override
    protected void reduce(Text key, Iterable<PhoneBean> values, Context context) throws IOException, InterruptedException {
        long sum1 = 0;
        long sum2 = 0;
        for (PhoneBean p:values){
            sum1 += p.getUpFlow();
            sum2 += p.getDownFlow();
        }
        PhoneBean p = new PhoneBean(sum1, sum2);
        context.write(key, p);
    }
}
