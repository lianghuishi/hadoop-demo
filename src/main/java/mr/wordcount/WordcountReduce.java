package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


/**
 * KEYIN, VALUEIN, 对应mapper输出的 KEYOUT, VALUEOUT
 * KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出类型
 * KEYOUT 是单词
 * VALUEOUT 是单词次数
 */
public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     *  key 入参key是一组相同单词kv对的key
     *  values 代表一组相同的key内对应的值，
     *  即一个key代表一个组，一组调一次reduce
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable value : values){
            count += value.get();
        }
        context.write(key , new IntWritable(count));
    }
}
