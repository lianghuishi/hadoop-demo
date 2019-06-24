package mr.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CopReduce extends Reducer<CopBean, Text, Text, CopBean> {
    @Override
    protected void reduce(CopBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(),key);
    }
}
