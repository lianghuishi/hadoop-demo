package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KETIN 默认情况下，是mr框架所读到的一行文本其实偏移量，LongWritable
 * VALUEIN 默认情况下，是mr框架所读到的一行文本内容，Text
 *
 * KEYOUT 是用户自定义逻辑处理完之后输出数据中的key，在此处是单词，String
 * VALUEOUT 是用户自定义逻辑处理完之后输出数据中的value，在此处是单词次数 IntWritable
 *
 */

/**
 * map阶段的业务逻辑就写在自定义的map()方法中
 * maptask会对每一行输入数据调用一次我们自定义的map()方法中
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");
        for (String word: words) {
            k.set(word);
            context.write(k , v);
        }
    }

}
