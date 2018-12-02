package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

public class TestHBase {
    private Admin admin = null;
    private Connection connection = null;
    private Configuration configuration = null;

    @Before
    public void init() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zookeeper的地址");
        configuration.set("hbase.zookeeper.property.clientPort", "zookeeper的端口号");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
        System.out.println(admin.tableExists(TableName.valueOf("表名")));

    }

    @After
    public void close() throws IOException {
        admin.close();
        connection.close();
    }

    /**
     * 旧API
     *
     * @throws IOException
     */
    public void tableExistOld() throws IOException {
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum", "zookeeper的地址");
        configuration.set("hbase.zookeeper.property.clientPort", "zookeeper的端口号");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        String tableName = "表名";
        System.out.println(admin.tableExists(tableName));
        admin.close();
    }

    /**
     * 新API
     *
     * @throws IOException
     */
    public boolean tableExistNew(String tableName) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zookeeper的地址");
        configuration.set("hbase.zookeeper.property.clientPort", "zookeeper的端口号");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param cf
     * @throws IOException
     */
    public void creteTable(String tableName, String... cf) throws IOException {
        //todo：先判断表是否存在
        //表名
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        Arrays.stream(cf).forEach((e) -> {
            //添加列族
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(e);
            //设置列族版本数
//            hColumnDescriptor.setMaxVersions(3);
            hTableDescriptor.addFamily(hColumnDescriptor);
        });
        //创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException {
        if (!tableExistNew(tableName)) {
            return;
        }
        //先使得表失效
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 插入操作
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @param value
     * @throws IOException
     */
    public void put(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        //新API,获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //旧API
        //HTable table = new HTable(configuration, tableName);
        //创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        //执行插入
        table.put(put);
        table.close();
    }

    /**
     * 删除操作
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws IOException
     */
    public void delete(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除全部版本
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //删除某版本,如果没有指定,默认删除最新的那个版本,该方法慎用！
//        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        table.delete(delete);
        table.close();
    }

    /**
     * 全表扫描
     * @param tableName
     * @throws IOException
     */
    public void scan(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构建扫描器,可以指定startRow和stopRow
        Scan scan = new Scan();
        //全表扫描
        ResultScanner scanner = table.getScanner(scan);
        //一个rowKey对应多个列族中的列的列的版本
        for (Result result : scanner){
            //一个cell(单元格)才是最小的数据单元
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("RK:"+CellUtil.cloneRow(cell)+",CF:"+CellUtil.cloneFamily(cell)+
                ",CN:"+ CellUtil.cloneQualifier(cell)+",VALUE:"+CellUtil.cloneValue(cell));
            }
        }
        table.close();
    }

    /**
     * 查询
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws IOException
     */
    public void get(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//        get.setMaxVersions();
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell cell : cells){
            System.out.println("RK:"+CellUtil.cloneRow(cell)+",CF:"+CellUtil.cloneFamily(cell)+
                    ",CN:"+ CellUtil.cloneQualifier(cell)+",VALUE:"+CellUtil.cloneValue(cell));
        }
        table.close();
    }
}
