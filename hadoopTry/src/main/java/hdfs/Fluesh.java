package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Fluesh {
    public static void writeFIle() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");

        Path path = new Path("hdfs://hadoop-1:9000/test/hello.txt");
//        FSDataOutputStream fos = fs.create(path,true);
//        fos.write("hello world1111".getBytes());
        FSDataOutputStream fos1= fs.append(path);
        fos1.write("hello world 112233".getBytes());
//        fos.hflush();
//        fos.close();//Failed to APPEND_FILE /test/hello.txt ,already the current lease holder.
        fos1.hflush();
        fos1.close();
    }

    public static void main(String[] args) throws Exception{
        writeFIle();
    }
}
