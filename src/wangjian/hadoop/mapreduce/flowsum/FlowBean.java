package wangjian.hadoop.mapreduce.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 1 on 2017/5/17.
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private String phoneNB;
    private long up_flow;
    private long down_flow;
    private long s_flow;

    //在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参数构造函数
    public FlowBean() {}

    public FlowBean(String phoneNB, long up_flow, long down_flow) {
        this.phoneNB = phoneNB;
        this.up_flow = up_flow;
        this.down_flow = down_flow;
        this.s_flow = up_flow + down_flow;
    }

    public void set(String phoneNB, long up_flow, long down_flow) {
        this.phoneNB = phoneNB;
        this.up_flow = up_flow;
        this.down_flow = down_flow;
        this.s_flow = up_flow + down_flow;
    }


    public String getPhoneNB() {
        return phoneNB;
    }

    public void setPhoneNB(String phoneNB) {
        this.phoneNB = phoneNB;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getDown_flow() {
        return down_flow;
    }

    public void setDown_flow(long down_flow) {
        this.down_flow = down_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }

    //将对象数据序列化到流中
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(phoneNB);
        out.writeLong(up_flow);
        out.writeLong(down_flow);
        out.writeLong(s_flow);

    }

    //从流中反序列出对象的数据
    //从数据流中读取对象字段时，必须跟序列化时的顺序保持一致
    @Override
    public void readFields(DataInput in) throws IOException {

        phoneNB = in.readUTF();
        up_flow = in.readLong();
        down_flow = in.readLong();
        s_flow = in.readLong();

    }

    @Override
    public String toString() {
        return phoneNB + "\t" + up_flow + "\t" + down_flow + "\t" + s_flow;
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        return this.s_flow > flowBean.s_flow ? -1 : 1;
    }
}
