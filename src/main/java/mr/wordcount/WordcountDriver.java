package mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {

    /**
     * 相当于一个yarn集群的客户端
     * 需要在此封装我们mr程序运行的相关参数，制定jar包
     * 最后提交给yarn
     */
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //1、获取job对象
        Job job = Job.getInstance(conf);
        //2、设置jar包
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReduce.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的kv类型(因为最终输出可能是reduce输出，也可能是mapper输出，如果reduce不配置的话，那么最终输出就是mapper)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job输入原始文件所在目录
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPaths(job, new Path("g:/t"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("g:/tt/"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yran运行
//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
