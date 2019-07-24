package compare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 全排序
 */
public class PhoneCompareDriver {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneCompareDriver.class);

        job.setMapperClass(PhoneCompareMapper.class);
        job.setReducerClass(PhoneCompareReduce.class);

        job.setMapOutputKeyClass(PhoneCompareBean.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PhoneCompareBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:/ideaProject/hadoop-demo/src/main/infile/phone"));
        FileOutputFormat.setOutputPath(job, new Path("g:/tt22/"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}

/**
 13422162300,100,200,1
 13422162300,100,200,2
 13422162301,100,200,4
 13422162388,100,200,3
 13422162366,100,200,9
 13422162333,100,200,9
 13422162388,100,200,11
 */

/**
 11	PhoneCompareBean{phone='13422162388', upFlow=100, downFlow=200, order=11}
 9	PhoneCompareBean{phone='13422162366', upFlow=100, downFlow=200, order=9}
 9	PhoneCompareBean{phone='13422162333', upFlow=100, downFlow=200, order=9}
 4	PhoneCompareBean{phone='13422162301', upFlow=100, downFlow=200, order=4}
 3	PhoneCompareBean{phone='13422162388', upFlow=100, downFlow=200, order=3}
 2	PhoneCompareBean{phone='13422162300', upFlow=100, downFlow=200, order=2}
 1	PhoneCompareBean{phone='13422162300', upFlow=100, downFlow=200, order=1}
 */