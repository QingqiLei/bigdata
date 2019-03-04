package example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private String connectString = "zk1:2181,zk2:2181,zk3:2181";
    private int sessionTimeout=2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";
    private volatile ArrayList<String> serversList = new ArrayList<String>();

    public void getConnect() throws IOException{
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try{
                    getServerList();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
    }
    public void getServerList() throws Exception{
        List<String> children = zk.getChildren(parentNode, true);
        ArrayList<String> servers = new ArrayList<String>();

        for(String child: children){
            byte[] data = zk.getData(parentNode+"/"+child,false , null);
            servers.add(new String(data));
        }
        serversList = servers;
        serversList.forEach(s-> System.out.println(s));
    }
    public void business() throws Exception{
        System.out.println("client is working>>>>");
        Thread.sleep(Long.MAX_VALUE);

    }
    public static void main(String[] args) throws Exception{
        DistributeClient client = new DistributeClient();
        client.getConnect();
        client.getServerList();
        client.business();

    }
}
