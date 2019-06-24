package mr.object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ObjectDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ObjectDriver.class);

        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(ObjectMapper.class);
        job.setReducerClass(ObjectReduce.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserBean.class);

        //指定最终输出的kv类型(因为最终输出可能是reduce输出，也可能是mapper输出，如果reduce不配置的话，那么最终输出就是mapper)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserBean.class);

        //指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yran运行
//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
