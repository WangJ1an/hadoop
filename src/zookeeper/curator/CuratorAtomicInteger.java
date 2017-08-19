package zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

/**
 * Created by 1 on 2017/6/30.
 */
public class CuratorAtomicInteger {

    static final String CONNECT_ADDR = "192.168.2.205:2181";
    static final int SESSION_OUTTIME = 1000;

    public static CuratorFramework createCuratorFramework() {

        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(new ExponentialBackoffRetry(1000,10))
                .build();

        return cf;
    }

    public static void main(String[] args) throws Exception {
        CuratorFramework cf = createCuratorFramework();

        cf.start();

//        cf.delete().forPath("/super");

        DistributedAtomicInteger dai = new DistributedAtomicInteger(cf,"/super",new RetryNTimes(10,1000));

//        dai.forceSet(0);
        dai.increment();
        AtomicValue<Integer> value = dai.get();

        System.out.println(value.succeeded());

        System.out.println(value.postValue());
        System.out.println(value.preValue());
    }

}
