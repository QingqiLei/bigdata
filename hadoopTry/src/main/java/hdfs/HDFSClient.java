package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.URI;
import java.util.Arrays;

public class HDFSClient {

    public static void uploadFile(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://hadoop-1:9000");
//        FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), configuration, "root");
        fs.copyFromLocalFile(new Path("/home/chauncey/Desktop/tt.deb"), new Path("/test"));

        fs.close();

    }

    public static void getFIleSystem() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        System.out.println(fs.toString());
    }

    public static void getFile() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");

        fs.copyToLocalFile(new Path("/test/tt.deb"), new Path("/home/chauncey/Desktop/ttt.deb"));
    }

    public static void mkDir() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        fs.mkdirs(new Path("/test/tiantant/s/q/t"));
        fs.close();
    }

    public static void deleteAtHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        fs.delete(new Path("/test/tt.deb"), true);
        fs.close();
    }

    public static void renameATHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        fs.rename(new Path("/test/t.deb"), new Path("/test/r.deb"));
        fs.close();
    }

    public static void readFileAtHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            System.out.println(status.getPath().getName() + " " + status.getBlockSize() / 1024 / 1024 + "MB");
            System.out.println(status.getLen() / 1024 / 1024);
            System.out.println(status.getPermission());
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation bl : blockLocations) {
                System.out.println(bl.getOffset());
                String[] hosts = bl.getHosts();
                System.out.println(Arrays.toString(hosts));
            }
        }
    }


    public static void readFolderAtHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        fs.listStatus(new Path("/test"));
        FileStatus[] listStatus = fs.listStatus(new Path("/test"));
        for (FileStatus status : listStatus) {
            if (status.isFile())
                System.out.println("f--: " + status.getPath().getName());
            else System.out.println("d--: " + status.getPath().getName());
        }
        fs.close();
    }

    public static void main(String[] args) throws Exception {
        readFolderAtHDFS();
    }
}
