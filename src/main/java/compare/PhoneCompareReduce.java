package compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneCompareReduce extends Reducer<PhoneCompareBean, LongWritable, LongWritable, PhoneCompareBean> {

    @Override
    protected void reduce(PhoneCompareBean key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for(LongWritable order:values){
            context.write(order, key);
        }
    }

}
