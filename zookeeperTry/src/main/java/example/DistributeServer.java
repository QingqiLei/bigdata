package example;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {
    private String connectString = "zk1:2181,zk2:2181,zk3:2181";
    private int sessionTimeout=2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    // 创建到zk 的客户端连接
    public void getConnect() throws IOException{
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            // 收到时间通知后的回调函数
            public void process(WatchedEvent watchedEvent) {
                     try{
                         zk.getChildren("/", true);
                     }catch (Exception e){
                         e.printStackTrace();
                     } }
        });
    }

    public void registServer(String hostname) throws Exception{
        String create = zk.create(parentNode+"/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+ "is online"+create );
    }

    public void business() throws Exception{
        System.out.println(" 来接课>>>>>");
        Thread.sleep(Long.MAX_VALUE);
    }
    public static void main(String[] args)throws Exception{
        DistributeServer server = new DistributeServer();
        server.getConnect();
        server.registServer(args[0]);
        server.business();
    }
}
