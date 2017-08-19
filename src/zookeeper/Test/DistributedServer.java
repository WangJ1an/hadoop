package zookeeper.Test;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * Created by 1 on 2017/7/30.
 */
public class DistributedServer {
    private static final String CONNECT_ADDR = "192.168.2.201:2181,192.168.2.202:2181,192.168.2.203:2181";
    private static final int SESSION_OUTTIME = 5000;
    private static final String PARENT_PATH = "/servers";
    private static CuratorFramework cf;

    private String serverName;

    static {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,10);
        cf =  CuratorFrameworkFactory.builder().connectionTimeoutMs(SESSION_OUTTIME)
                .connectString(CONNECT_ADDR)
                .retryPolicy(retryPolicy)
                .build();

        cf.start();
        PathChildrenCache cache = new PathChildrenCache(cf,PARENT_PATH,true);
        try {
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                switch (pathChildrenCacheEvent.getType()) {
                    case CHILD_ADDED:
                        System.out.println(pathChildrenCacheEvent.getData().getPath()+"上线啦");
                        break;
                    case CHILD_REMOVED:
                        System.out.println(pathChildrenCacheEvent.getData().getPath()+"下线啦----------");
                        break;
                    default:
                        break;
                }
            }
        });
    }

    public DistributedServer(String name) {
        this.serverName = name;
    }

    public void regester() throws Exception {
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(PARENT_PATH+"/"+this.serverName);
        Thread.sleep(1000);
    }

    public void leave() throws Exception {
        cf.delete().forPath(PARENT_PATH+"/"+this.serverName);
    }


    public static void main(String[] args) throws Exception {
        DistributedServer server1 = new DistributedServer("server1");
        DistributedServer server2 = new DistributedServer("server2");
        server1.regester();
        server2.regester();

        Thread.sleep(5000);
        server1.leave();
        Thread.sleep(5000);
        server2.leave();
        Thread.sleep(5000);
    }
}
