package wangjian.hadoop.rpc;

/**
 * Created by 1 on 2017/5/15.
 */
public interface LoginServiceInterface {

     public static final long versionID = 1L;
     public String login(String username, String password);
}
