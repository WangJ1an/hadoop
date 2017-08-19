package wangjian.hadoop.mapreduce.groupComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by 1 on 2017/7/6.
 */
public class ItemidGroupingComparator extends WritableComparator {
    public ItemidGroupingComparator() {

        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;

        //将item_id相同的bean都视为相同，从而聚合为一组
        return abean.getItemid().compareTo(bbean.getItemid());

    }
}
