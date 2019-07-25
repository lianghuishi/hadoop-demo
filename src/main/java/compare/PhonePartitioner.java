package compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  k 和 v 都是mapper 的输出端
 */
public class PhonePartitioner extends Partitioner <PhoneCompareBean, LongWritable> {
    @Override
    public int getPartition(PhoneCompareBean phoneCompareBean, LongWritable order, int numPartitions) {
        // 1 获取手机号码前三位
        String preNum = phoneCompareBean.getPhone().substring(0, 3);
        int partition = 4;
        // 2 根据手机号归属地设置分区
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }
        /*else {
            partition = 4;
        }*/
        return partition;

/*        // 1 获取手机号码前三位
        String preNum = text.toString().substring(0, 3);
        int partition = 4;
        // 2 根据手机号归属地设置分区
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;*/
    }
}
