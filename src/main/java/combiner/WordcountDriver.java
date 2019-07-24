package combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {

    /**
     * map之后的汇总 如果数据合并和reduce的一样，那么直接把WordcountReduce直接放进去也可以
     * job.setCombinerClass(WordcountCombiner.class);
     */
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //1、获取job对象
        Job job = Job.getInstance(conf);
        //2、设置jar包
        job.setJarByClass(WordcountDriver.class);

        job.setCombinerClass(WordcountCombiner.class);

        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReduce.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的kv类型(因为最终输出可能是reduce输出，也可能是mapper输出，如果reduce不配置的话，那么最终输出就是mapper)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);
        //指定job输入原始文件所在目录
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPaths(job, new Path("D:/ideaProject/hadoop-demo/src/main/infile/t"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("g:/tt16/"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yran运行
//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}

/**
 * Map-Reduce Framework
 * 		Map input records=5
 * 		Map output records=13
 * 		Combine input records=13
 * 		Combine output records=8
 */
