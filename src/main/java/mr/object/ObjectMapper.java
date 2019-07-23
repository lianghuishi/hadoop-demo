package mr.object;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ObjectMapper extends Mapper<LongWritable, Text, Text, UserBean> {

    UserBean user = new UserBean();

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        String[] split = line.toString().split(",");

        String name = split[0];
        Double amt = new Double(split[1]);

        user.setName(name);
        user.setAmt(amt);

        context.write(new Text(name),user);

    }

}
