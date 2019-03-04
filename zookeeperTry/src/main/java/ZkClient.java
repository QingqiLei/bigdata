import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ZkClient {
    private String connectString = "zk-1:2181,zk-2:2181,zk-3:2181";
    private int sessionTimeout=2000;
    private ZooKeeper zkClient = null;



    public void initZk() throws Exception{
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("event:   "+event.getType() + "---"+ event.getPath());
                try {
                    zkClient.getChildren("/hbase" ,true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void create() throws Exception{
        zkClient.create("/qingq11","ss.avi".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void getChildren() throws Exception{
        List<String> children = zkClient.getChildren("/hbase", true);
        for(String child: children){
            System.out.println(child );
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    public void exist() throws Exception{
        Stat stat = zkClient.exists("/eclipse",true);
        System.out.println(stat == null ? "not exist":"exist");
    }
    public static void main(String[] args) throws Exception{
        ZkClient zkc = new ZkClient();
        zkc.initZk();
//        zkc.create();
        zkc.getChildren();
//        zkc.exist();

    }
}
