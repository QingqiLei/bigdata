package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class IOToHDFS {
    public static void putFIleTOHDFS() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        // fs.create
        FSDataOutputStream fos = fs.create(new Path("/test/output/spark-2.4.0-bin-hadoop2.7.tgz"));
        FileInputStream fis = new FileInputStream(new File("/usr/local/spark-2.4.0-bin-hadoop2.7.tgz"));
        try {
            IOUtils.copyBytes(fis, fos, conf);
        } catch (Exception e) {

        } finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    // 从hdfs 上下载, 需要用hdfs的输入流(inputStream)
    public static void getFileFromHDFS() throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        FSDataInputStream fis = fs.open(new Path("/test/output/4.txt"));
        FileOutputStream fos = new FileOutputStream(new File("/usr/local/4.txt"));
        try {
            IOUtils.copyBytes(fis, fos, conf);
        } catch (Exception e) {

        } finally {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    public static void getFileFromHDFSSeek1() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf,"root");
        // download, fs.open()
        FSDataInputStream fis = fs.open(new Path("/test/output/spark-2.4.0-bin-hadoop2.7.tgz"));
        FileOutputStream fos = new FileOutputStream(new File("/home/chauncey/Desktop/spark-2.4.0-bin-hadoop2.7.tgz.part1"));
        byte[] buf = new byte[1024];
        for(int i = 0; i < 1024*128;i++){
            fis.read(buf);
            fos.write(buf);
        }
        try {
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }catch (Exception e){

        }
    }
    public static void getFileFromHDFSSeek2() throws Exception{
        // get File System
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf,"root");

        // get input stream
        FSDataInputStream fis = fs.open(new Path("/test/output/spark-2.4.0-bin-hadoop2.7.tgz"));

        // get output stream
        FileOutputStream fos = new FileOutputStream(new File("/home/chauncey/Desktop/spark-2.4.0-bin-hadoop2.7.tgz.part2"));
        // join the streams
        fis.seek(1024*1024*128);
        IOUtils.copyBytes(fis, fos, conf);

        // close streams
        try{
            IOUtils.copyBytes(fis, fos, conf);

        }catch (Exception e){
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    public static void main(String[] args) throws Exception {
        getFileFromHDFSSeek2();
    }


}
