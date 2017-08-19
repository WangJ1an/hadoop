package wangjian.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by 1 on 2017/5/15.
 */
public class LoginController {

    public static void main(String[] args) throws IOException {

        LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class,
                1L, new InetSocketAddress("MetaServer",10000),new Configuration());

        String result = proxy.login("wangjian","1234");

        System.out.println(result);

    }


}
