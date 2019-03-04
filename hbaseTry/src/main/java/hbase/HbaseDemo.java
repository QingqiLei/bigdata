package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {
    static Configuration conf;
    static Connection conn;
    static Admin admin;
    static{
        conf = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        try {
				System.out.println(isTableExist("student"));
				createTable("person", "basic_info", "job", "heathy");
//				dropTable("person");
				addRowData("person", "1001", "basic_info", "name", "Nick");
				addRowData("person", "1001", "basic_info", "sex", "Male");
				addRowData("person", "1001", "basic_info", "age", "18");
				addRowData("person", "1001", "job", "dept_no", "7981");
            getRow("person","1001");
//				deleteMultiRow("person", "person");
//            getAllRows("person");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean  isTableExist(String tableName) throws IOException {
       return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, String... columnFamily) throws Exception{
        if(isTableExist(tableName)){
            System.out.println("table: "+tableName+" already exists!!!!!!!!!");
        }else{
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf: columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));

            }
            admin.createTable(descriptor);
            System.out.println("table"+tableName+" created  !!!!!!");
        }
    }

    public static void dropTable(String tableName) throws Exception{
        if(isTableExist(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName+" deleted!!!!!!!!!!!!");
        }else{
            System.out.println(tableName+" does not exist");
        }
    }
    public static void addRowData(String tableName , String rowKey, String columnFamily, String column,String value) throws Exception{
        if(isTableExist(tableName)){
            Table  table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
            System.out.println("yes!!!!!!!!!!");
        }else{
            System.out.println(tableName +" does not exist!!!!!!!!!!!");
        }
    }

    // 在原有的value后面追加新的value，  "a" + "bc"  -->  "abc"
    public static void appendData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Append append = new Append(Bytes.toBytes(rowKey));
        append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.append(append);
        table.close();
        conn.close();
    }


    public static void getRow(String tableName, String rowKey) throws Exception{
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);         //  输出结果
        for (Cell cell : result.rawCells()) {
            System.out.print(Bytes.toString(CellUtil.cloneRow(cell))+"  ");
            //得到列族
            System.out.print(Bytes.toString(CellUtil.cloneFamily(cell))+"  ");
            System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+"  ");
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void deleteMultiRow(String tableName, String... rows) throws Exception{
        if(isTableExist(tableName)){
            Table table = conn.getTable(TableName.valueOf(tableName));
            List<Delete> deleteList = new ArrayList<Delete>();
            for(String row: rows){
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            table.delete(deleteList);
            table.close();
        }else{
            System.out.println(tableName +" does not exist!!!!!!!!!!!");
        }
    }

    public static void getAllRows(String tableName) throws Exception{
        if(isTableExist(tableName)){
            Table table = conn.getTable(TableName.valueOf(tableName));
           Scan scan = new Scan();
           ResultScanner resultScanner = table.getScanner(scan);
           for(Result result: resultScanner){
               Cell[] cells = result.rawCells();
               for(Cell cell:cells){
                   System.out.print(Bytes.toString(CellUtil.cloneRow(cell))+"  ");
                   //得到列族
                   System.out.print(Bytes.toString(CellUtil.cloneFamily(cell))+"  ");
                   System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+"  ");
                   System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
               }
           }
            table.close();
        }else{
            System.out.println(tableName +" does not exist!!!!!!!!!!!");
        }
    }
}
