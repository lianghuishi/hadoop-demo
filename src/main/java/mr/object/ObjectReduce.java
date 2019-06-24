package mr.object;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ObjectReduce extends Reducer <Text, UserBean, Text, UserBean>{

    @Override
    protected void reduce(Text key, Iterable<UserBean> values, Context context) throws IOException, InterruptedException {

        Double amt = 0.0;
        for(UserBean user:values){
            amt += user.getAmt();
        }

        UserBean user = new UserBean();
        user.setName(key.toString());
        user.setAmt(amt);
        context.write(key,user);

    }
}
