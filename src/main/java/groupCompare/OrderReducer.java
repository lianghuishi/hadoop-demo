package groupCompare;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    //这里所谓的key相同，只是说比较的时候order_id相同则视为key相同，但是对象里面的数据是不一样的

    /**
     * reduce向map端拉取数据之前，数据会进行一次整体的归并排序。
     * reduce拉取数据时候会将相同key为一组的数据分成一组，然后每组分别进行操作
     * 如果key是直接可以比较的数字或者字符串，那么整体归并排序的时候会按照字典顺序排序
     * 如果想要自定义排序规则，或者key是对象，那么就要通过编写GroupingComparator来自定义key排序的规则
    */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
        /*for (NullWritable nullWritable:values){
            context.write(key, NullWritable.get());
        }*/

    }

}
