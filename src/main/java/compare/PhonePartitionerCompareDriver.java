package compare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 区内排序（增加多个分区）
 * // 加载自定义分区类
 * job.setPartitionerClass(ProvincePartitioner.class);
 * // 设置Reducetask个数
 * job.setNumReduceTasks(5);
 */
public class PhonePartitionerCompareDriver {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhonePartitionerCompareDriver.class);

         // 加载自定义分区类
         job.setPartitionerClass(PhonePartitioner.class);
         // 设置Reducetask个数
         job.setNumReduceTasks(5);

        job.setMapperClass(PhoneCompareMapper.class);
        job.setReducerClass(PhoneCompareReduce.class);

        job.setMapOutputKeyClass(PhoneCompareBean.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PhoneCompareBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:/ideaProject/hadoop-demo/src/main/infile/phone2"));
        FileOutputFormat.setOutputPath(job, new Path("g:/ttt3/"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
/**
     13622162300,100,200,1
     13722162300,100,200,2
     13822162301,100,200,4
     13622162388,100,200,3
     13622162366,100,200,9
     13422162333,100,200,9
     13422162388,100,200,11
 */