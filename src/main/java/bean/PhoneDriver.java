package bean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneDriver {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneDriver.class);

        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:/ideaProject/hadoop-demo/src/main/infile/phone"));
        FileOutputFormat.setOutputPath(job, new Path("g:/tt6/"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
