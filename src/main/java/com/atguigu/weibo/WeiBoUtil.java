package com.atguigu.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WeiBoUtil {

    private static Configuration configuration = HBaseConfiguration.create();

    static {
        configuration.set("hbase.zookeeper.quorum", "zookeeper的地址");
    }

    /**
     * 创建命名空间
     *
     * @param ns
     * @throws IOException
     */
    public static void createNamespace(String ns) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param versions
     * @param cfs
     * @throws IOException
     */
    public void createTable(String tableName, int versions, String... cfs) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族信息
        Arrays.stream(cfs).forEach((cf) -> {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        });
        admin.createTable(hTableDescriptor);

        admin.close();
        connection.close();
    }

    /**
     * 发布微博
     * 1.更新微博内容表数据
     * 2.更新收件箱表数据
     * --获取当前操作人的粉丝
     * --去往收件箱表依次更新数据
     *
     * @param uid
     * @param content
     */
    public void createData(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取content表
        Table contentTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        //拼接RK
        String rowKey = uid + "_" + System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        //往内容表添加步骤
        contentTable.put(put);
        //获取用户关系表
        Table relationsTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relationsTable.get(get);
        Cell[] cells = result.rawCells();
        List<Put> puts = new ArrayList<>();
        //如果没有粉丝就不需要往inbox添加数据了
        if (cells.length <= 0)
            return;
        Arrays.stream(cells).forEach((cell) -> {
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            Put p = new Put(cloneQualifier);
            p.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            puts.add(put);
        });
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        //更新fans收件箱表
        inboxTable.put(puts);

        contentTable.close();
        relationsTable.close();
        inboxTable.close();

        connection.close();
    }

    /**
     * 关注用户
     * 1.用户关系表
     * --添加操作人的attends
     * --添加被关注人的fans
     * 2.在收件箱中
     * --在收件箱表中添加被关注者在内容表的rowKey
     *
     * @param uid
     * @param uids
     */
    public void attend(String uid, String... uids) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取content表
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

        Put relaPut = new Put(Bytes.toBytes(uid));
        List<Put> puts = new ArrayList<>();
        Arrays.stream(uids).forEach((u) -> {
            //添加操作人的attends
            relaPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(u), Bytes.toBytes(u));
            //添加被关注人的fans
            Put fansPut = new Put(Bytes.toBytes(u));
            fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);
        });
        puts.add(relaPut);
        //批量插入
        relaTable.put(puts);
        Put inboxPut = new Put(Bytes.toBytes(uid));
        //获取内容表中被关注者的rowKey
        for (String u : uids) {
            //因为‘uid_’<‘uid|’所以uid_ts都会被扫描到
            Scan scan = new Scan(Bytes.toBytes(u), Bytes.toBytes(u + "|"));
            ResultScanner results = contTable.getScanner(scan);
            for (Result result : results) {
                byte[] row = result.getRow();
                String rowKey = Bytes.toString(row);
                //获取发布微博时候的时间戳
                String[] split = rowKey.split("_");
                //控制版本
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(u), Long.parseLong(split[1]), row);
            }
        }
        inboxTable.put(inboxPut);

        contTable.close();
        relaTable.close();
        inboxTable.close();

        connection.close();
    }

    /**
     * 取关
     * 1.用户关系表
     * --删除操作者关注列族中待取关用户
     * --删除待取关用户fans列族中的操作者
     * 2.收件箱表
     * --删除操作者的待取关用户的信息
     *  注：这里的删除操作用的都是addColumns
     * @param uid
     * @param uids
     */
    public static void delAttend(String uid, String... uids) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

        Delete attendDel = new Delete(Bytes.toBytes(uid));
        List<Delete> relaDels = new ArrayList<>();
        //分别删除关系表attend和fans列族中的待取关用户和操作者(取关者)用户
        Arrays.stream(uids).forEach((u) -> {
            attendDel.addColumns(Bytes.toBytes("attends"), Bytes.toBytes(u));
            Delete fansDel = new Delete(Bytes.toBytes(u));
            fansDel.addColumns(Bytes.toBytes("fans"),Bytes.toBytes(uid));
            relaDels.add(fansDel);
        });
        relaDels.add(attendDel);
        //执行删除
        relaTable.delete(relaDels);

        Delete inboxDel = new Delete(Bytes.toBytes(uid));
        //删除操作者收件箱表中的待取关用户信息
        Arrays.stream(uids).forEach((u) -> {
            inboxDel.addColumns(Bytes.toBytes("info"), Bytes.toBytes(u));
        });
        inboxTable.delete(inboxDel);

        relaTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 获取某个人所有的微博内容
     * @param uid
     */
    public static void getData(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));

        Scan scan = new Scan();
        //使用过滤器的方式进行过滤
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_")));

        ResultScanner scanner = contTable.getScanner(scan);
        for (Result result : scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:" + CellUtil.cloneRow(cell)+
                        ",Content:" + CellUtil.cloneValue(cell));
            }
        }

        contTable.close();
        connection.close();
    }

}
