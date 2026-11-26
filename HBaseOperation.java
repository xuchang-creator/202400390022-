import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOperation {
    private static Connection connection;
    private static Admin admin;

    // 初始化连接
    public static void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    // 关闭连接
    public static void close() throws IOException {
        if (admin != null) admin.close();
        if (connection != null) connection.close();
    }

    // 1. 创建表，存在则先删除
    public static void createTable(String tableName, String[] fields) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
            System.out.println("已删除原有表: " + tableName);
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String field : fields) {
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of(field);
            builder.setColumnFamily(cf);
        }
        admin.createTable(builder.build());
        System.out.println("表创建成功: " + tableName);
    }

    // 2. 添加记录
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] cfCol = fields[i].split(":");
            if (cfCol.length == 2) {
                put.addColumn(Bytes.toBytes(cfCol[0]), Bytes.toBytes(cfCol[1]), Bytes.toBytes(values[i]));
            } else {
                put.addColumn(Bytes.toBytes(cfCol[0]), Bytes.toBytes(""), Bytes.toBytes(values[i]));
            }
        }
        table.put(put);
        table.close();
        System.out.println("数据插入成功");
    }

    // 3. 浏览某一列数据
    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String[] cfCol = column.split(":");
        if (cfCol.length == 2) {
            scan.addColumn(Bytes.toBytes(cfCol[0]), Bytes.toBytes(cfCol[1]));
        } else {
            scan.addFamily(Bytes.toBytes(cfCol[0]));
        }

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            System.out.print("行键: " + rowKey + " | ");
            for (Cell cell : result.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.print(col + ": " + val + " ");
            }
            System.out.println();
        }
        scanner.close();
        table.close();
    }

    // 4. 修改数据
    public static void modifyData(String tableName, String row, String column, String newValue) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        String[] cfCol = column.split(":");
        put.addColumn(Bytes.toBytes(cfCol[0]), Bytes.toBytes(cfCol[1]), Bytes.toBytes(newValue));
        table.put(put);
        table.close();
        System.out.println("数据修改成功");
    }

    // 5. 删除行
    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
        table.close();
        System.out.println("行删除成功");
    }

    // 测试主函数
    public static void main(String[] args) throws IOException {
        init();
        
        // 测试创建表
        String[] fields = {"info", "score"};
        createTable("StudentTest", fields);
        
        // 测试插入数据
        String[] cols = {"info:S_Name", "info:S_Age", "score:Math"};
        String[] vals = {"Zhangsan", "23", "86"};
        addRecord("StudentTest", "2015001", cols, vals);
        
        // 测试扫描列
        System.out.println("\n扫描 info 列族:");
        scanColumn("StudentTest", "info");
        
        // 测试修改
        modifyData("StudentTest", "2015001", "info:S_Age", "24");
        
        // 测试删除
        deleteRow("StudentTest", "2015001");
        
        close();
    }
}
