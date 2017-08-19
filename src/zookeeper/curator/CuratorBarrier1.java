package zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Random;

/**
 * Created by 1 on 2017/7/1.
 */
public class CuratorBarrier1 {

    static final String CONNECT_ADDR = "192.168.2.205:2181";
    static final int SESSION_OUTTIME = 5000;

    public static CuratorFramework createCuratorFramework() {
        return CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .connectionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(new ExponentialBackoffRetry(1000,10))
                .build();
    }

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        CuratorFramework cf = createCuratorFramework();
                        cf.start();
                        DistributedDoubleBarrier doubleBarrier = new DistributedDoubleBarrier(cf, "/super", 5);
                        Thread.sleep(1000 * new Random().nextInt(5));
                        System.out.println(Thread.currentThread().getName() + "已经准备");
                        doubleBarrier.enter();
                        System.out.println("同时开始运行");
                        Thread.sleep(1000 * new Random().nextInt(5));
                        System.out.println(Thread.currentThread().getName() + "运行完毕");
                        doubleBarrier.leave();
                        System.out.println("同时退出");

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },"t"+i).start();
        }

    }

}
