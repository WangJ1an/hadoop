package zookeeper.curator.culster;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * Created by 1 on 2017/7/1.
 */
public class Test {
    static final String CONNECT_ADDR = "192.168.2.205:2181";
    static final int SESSION_OUTTIME = 5000;

    public static void main(String[] args) throws Exception {
        CuratorWatcher watcher = new CuratorWatcher();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,10);
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .connectionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();
        cf.start();

//        Thread.sleep(3000);
//        System.out.println(cf.getChildren().forPath("/super").get(0));


        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c1","c1内容".getBytes());
        Thread.sleep(1000);
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c2","c2内容".getBytes());
        Thread.sleep(1000);

        String res1 = new String(cf.getData().forPath("/super/c1"));
        System.out.println(res1);

        Thread.sleep(1000);
        cf.setData().forPath("/super/c2","修改的新c2内容".getBytes());
        String res2 = new String(cf.getData().forPath("/super/c2"));
        System.out.println(res2);

    }
}
