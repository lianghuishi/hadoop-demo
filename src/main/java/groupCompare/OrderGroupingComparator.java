package groupCompare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//在reduce端利用groupingcomparator将订单id相同的kv聚合成组,相当于sql的group by 相同的id只有一条记录，如果最大值在前面那么那条记录就是最大值
public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        //只要orderid相同就返回0，则被视为key相同（其实对象里面的其他值是不同的），那么就会被分配到同一组数据中处理
        if (aBean.getOrder_id() > bBean.getOrder_id()) {
            result = 1;
        } else if (aBean.getOrder_id() < bBean.getOrder_id()) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }
}
