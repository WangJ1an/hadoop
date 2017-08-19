package zookeeper.curator.culster;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;


/**
 * Created by 1 on 2017/7/1.
 */
public class CuratorWatcher {
    static final String PARENT_PATH = "/super";

    static final String CONNECT_ADDR = "192.168.2.205:2181";

    static final int SESSION_OUTTIME = 5000;

    public CuratorWatcher() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,10);
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .connectionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();
        cf.start();

        if (cf.checkExists().forPath(PARENT_PATH) == null) {
            cf.create().withMode(CreateMode.PERSISTENT).forPath(PARENT_PATH,"super init".getBytes());
        }

        PathChildrenCache cache = new PathChildrenCache(cf,PARENT_PATH,true);

        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                switch(pathChildrenCacheEvent.getType()) {
                    case CHILD_ADDED:
                        System.out.println("CHILD_ADDED: "+pathChildrenCacheEvent.getData().getPath());
                        System.out.println("CHILD_ADDED: "+new String(pathChildrenCacheEvent.getData().getData()));
                        break;
                    case CHILD_UPDATED:
                        System.out.println("CHILD_UPDATED: "+pathChildrenCacheEvent.getData().getPath());
                        System.out.println("CHILD_UPDATED: "+new String(pathChildrenCacheEvent.getData().getData()));
                        break;
                    case CHILD_REMOVED:
                        System.out.println("CHILD_REMOVED: "+pathChildrenCacheEvent.getData().getPath());
                        System.out.println("CHILD_REMOVED: "+new String(pathChildrenCacheEvent.getData().getData()));
                        break;
                    default:
                        break;
                }
            }
        });

    }
}
