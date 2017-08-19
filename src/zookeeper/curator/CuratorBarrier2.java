package zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by 1 on 2017/7/1.
 */
public class CuratorBarrier2 {

    static final String CONNECT_ADDR = "192.168.2.205:2181";
    static final int SESSION_OUTTIME = 5000;

    public static CuratorFramework createCuratorFramework() {
        return CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .connectionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(new ExponentialBackoffRetry(1000,10))
                .build();
    }

    static DistributedBarrier barrier = null;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        CuratorFramework cf = createCuratorFramework();
                        cf.start();
                        barrier = new DistributedBarrier(cf, "/super");
                        System.out.println(Thread.currentThread().getName() + "设置barrier");
                        barrier.setBarrier();
                        barrier.waitOnBarrier();
                        System.out.println("开始执行程序----------------");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },"t"+i).start();
        }

        Thread.sleep(5000);
        barrier.removeBarrier();
    }
}
