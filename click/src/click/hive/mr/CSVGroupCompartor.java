package click.hive.mr;

import click.hive.mr.mrbean.PageViewsBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by 1 on 2017/7/19.
 */
public class CSVGroupCompartor extends WritableComparator{
    public CSVGroupCompartor() {
        super(PageViewsBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PageViewsBean pvb1 = (PageViewsBean) a;
        PageViewsBean pvb2 = (PageViewsBean) b;
        String pvb1_Session = pvb1.getSession();
        String pvb2_Session = pvb2.getSession();
        return pvb1_Session.compareTo(pvb2_Session);
    }
}
