package hdfs;

import org.apache.hadoop.net.DNSToSwitchMapping;
import java.util.ArrayList;
import java.util.List;

public class AutoRack implements DNSToSwitchMapping {
    public List<String> resolve(List<String> ips) {
        //ips : 1 hadoop-1 172.18.0.2

        ArrayList<String> lists = new ArrayList<String>();
        int ipnumber = 0;
        if (ips != null && ips.size() > 0) {
            for (String ip : ips) {

                if (ip.startsWith("hadoop")) {
                    String ipnum = ip.substring(6);
                    ipnumber = Integer.parseInt(ipnum);

                } else if (ip.startsWith("172")) {
                    int index = ip.lastIndexOf(".");
                    String ipnum = ip.substring(index + 1);
                    ipnumber = Integer.parseInt(ipnum);
                }
                if (ipnumber < 5) {
                    lists.add("/rack1/" + ipnumber);
                } else {
                    lists.add("/rack2/" + ipnumber);
                }
            }


        }
        return lists;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> names) {

    }
}
