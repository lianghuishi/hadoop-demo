package mr.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CopMapper extends Mapper<LongWritable, Text, CopBean, Text> {

    CopBean bean = new CopBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String name = split[0];
        Double amt = new Double(split[1]);

        bean.setName(name);
        bean.setAmt(amt);

        /**
         * WritableComparable 中的compareTo 是针对从mapper输出到reduce中的key做排序的
         * 所以要想排序成功就要把需要排序的bean放到maper输出的key，然后reduce就可以根据这个key排序
         */
        context.write(bean, new Text(name));

    }
}
