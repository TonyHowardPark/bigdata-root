package com.sdiread.statistic.hbase.base;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import com.sdiread.statistic.common.util.FastJsonUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * @author fyn
 * @since 2019/7/10
 */
@Component("emrHBaseDao")
@Slf4j
public class EmrHBaseDao {

    private final static String ROW_KEY = "rowkey";
    private final static Object syncLock = new Object();

    // 关闭连接
    public static void close() {
        if (EmrHBaseconnectionFactory.connection != null) {
            try {
                EmrHBaseconnectionFactory.connection.close();
            } catch (IOException e) {
                log.error("Hbase  HconnectionFactory connection is error! ",e,e);
            }
        }
    }

    /**
     * @Descripton: 删除表
     * @param tableName
     */
    public void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        } catch (IOException e) {
            log.error("Hbase  dropTable is error,tableName:{} ",tableName,e,e);
        }
    }

    /**
     * @Descripton: 根据条件过滤查询
     * @param obj
     * @param param
     */
    public <T> List<T> queryScan(T obj, Map<String, String> param)throws Exception{
        List<T> objs = new ArrayList<T>();
        String tableName = HbaseUitls.getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            Scan scan = getScan(obj, param);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T)BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        } catch (Exception e) {
            log.error("Hbase  queryScan is error,tableName:{} ",tableName,e,e);
        }
        return objs;
    }

    /**
     * @Descripton: 根据rowkey查询
     * @param obj
     * @param rowkeys
     */
    public <T> List<T> get(T obj, String ... rowkeys) {
        List<T> objs = new ArrayList<T>();
        String tableName = HbaseUitls.getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return objs;
        }
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            List<Result> results = getResults(tableName, rowkeys);
            if (results.isEmpty()) {
                return objs;
            }
            for (int i = 0; i < results.size(); i++) {
                T bean = null;
                Result result = results.get(i);
                if (result == null || result.isEmpty()) {
                    continue;
                }
                try {
                    bean = HBaseBeanUtil.resultToBean(result, obj);
                    objs.add(bean);
                } catch (Exception e) {
                    log.error("Hbase  resultToBean  error：obj:{}",obj,e, e);
                }
            }
        }catch (Exception e){
            log.error("Hbase by rowkeys query error：rowKey:{}",rowkeys,e, e);

        }
        return objs;
    }


    /**
     * @Descripton: 保存实体对象
     * @param objs
     */
    public <T> boolean save(T ... objs) {
        List<Put> puts = new ArrayList<Put>();
        String tableName = "";
        try (Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            for (Object obj : objs) {
                if (obj == null) {
                    continue;
                }
                tableName = HbaseUitls.getORMTable(obj);
                // 表不存在，先获取family创建表
//                if(!admin.isTableAvailable(TableName.valueOf(tableName))){
//                    synchronized(syncLock) {
//                        createHbaseTable(obj, tableName, admin);
//                    }
//                }
                Put put = HBaseBeanUtil.beanToPut(obj);
                puts.add(put);
            }
        }catch (Exception e){
            log.error("Hbase save object error:{}！",objs,e);
        }
        return savePut(puts, tableName);
    }



    /**
     * @Descripton: 根据tableName保存
     * @param tableName
     * @param objs
     */
    public <T> void save(String tableName, T ... objs){
        List<Put> puts = new ArrayList<Put>();
        for (Object obj : objs) {
            if (obj == null) {
                continue;
            }
            try {
                Put put = HBaseBeanUtil.beanToPut(obj);
                puts.add(put);
            } catch (Exception e) {
                log.warn("", e);
            }
        }
        savePut(puts, tableName);
    }

    /**
     * @Descripton: 删除
     * @param obj
     * @param rowkeys
     */
    public <T> void delete(T obj, String... rowkeys) {
        String tableName = "";
        tableName = HbaseUitls.getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        List<Delete> deletes = new ArrayList<Delete>();
        for (String rowkey : rowkeys) {
            if (StringUtils.isBlank(rowkey)) {
                continue;
            }
            deletes.add(new Delete(Bytes.toBytes(rowkey)));
        }
        delete(deletes, tableName);
    }

    /**
     * @Descripton: 批量删除
     * @param deletes
     * @param tableName
     */
    private void delete(List<Delete> deletes, String tableName) {
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));) {
            if (StringUtils.isBlank(tableName)) {
                log.info("tableName is empty！");
                return;
            }
            table.delete(deletes);
        } catch (IOException e) {
            log.error("hbase delete error tableName:{}",tableName,e,e);
        }
    }

    /**
     * @Descripton: 根据tableName获取列簇名称
     * @param tableName
     */
    public List<String> getFamilys(String tableName) {
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            List<String> columns = new ArrayList<String>();
            if (table==null) {
                return columns;
            }
            HTableDescriptor tableDescriptor = table.getTableDescriptor();
            HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnDescriptor :columnDescriptors) {
                String columnName = columnDescriptor.getNameAsString();
                columns.add(columnName);
            }
            return columns;
        } catch (Exception e) {
            log.error("Query column cluster name failed！tableName:{}" ,tableName,e);
        }
        return new ArrayList<String>();
    }

    // 获取查询结果
    private List<Result> getResults(String tableName, String... rowkeys) {
        List<Result> resultList = new ArrayList<Result>();
        List<Get> gets = new ArrayList<Get>();
        for (String rowkey : rowkeys) {
            if (StringUtils.isBlank(rowkey)) {
                continue;
            }
            Get get = new Get(Bytes.toBytes(rowkey));
            gets.add(get);
        }
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));) {
            Result[] results = table.get(gets);
            Collections.addAll(resultList, results);
            return resultList;
        } catch (Exception e) {
            log.error("Hbase getResults  failed！tableName:{}" ,tableName,e);
            return resultList;
        }
    }
    
    /**
     * @Descripton: 根据条件过滤查询（大于等于）
     * @param obj
     * @param param
     */
    public <T> List<T> queryScanGreater(T obj, Map<String, String> param)throws Exception{
        List<T> objs = new ArrayList<T>();
        String tableName = HbaseUitls.getORMTable(obj);
        log.info("........................................create table name is :{}",tableName);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            Scan scan = new Scan();
            for (Map.Entry<String, String> entry : param.entrySet()){
                Class<?> clazz = obj.getClass();
                Field[] fields = clazz.getDeclaredFields();
                for (Field field : fields) {
                    if (!field.isAnnotationPresent(HbaseColumn.class)) {
                        continue;
                    }
                    field.setAccessible(true);
                    HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
                    String family = orm.family();
                    String qualifier = orm.qualifier();
                    if(qualifier.equals(entry.getKey())){
                        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(entry.getValue()));
                        scan.setFilter(filter);
                    }
                }
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T)BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        } catch (Exception e) {
            log.error("Filtering queries based on criteria failed,tableName:{}",tableName,e);
            throw new Exception(e);
        }
        return objs;
    }

    /**
     * 根据rowkey查询记录
     * @param obj
     * @param rowkey
     * @param <T>
     * @return
     */
    public <T> List<T> queryScanRowkey(T obj, String rowkey){
        List<T> objs = new ArrayList<T>();
        String tableName = HbaseUitls.getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        ResultScanner scanner = null;
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));Admin admin = EmrHBaseconnectionFactory.connection.getAdmin()){
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(rowkey));
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T)BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        }catch (Exception e){
            log.error("queryScanRowkey:Query failed！", e,e);
        }finally {
            if(scanner!=null){
                try {
                    scanner.close();
                } catch (Exception e) {
                    log.error("queryScan:Close flow exception！", e,e);
                }
            }
        }
        return objs;
    }

    /**
     * @Descripton: 创建表
     * @param tableName
     * @param familyColumn
     */
    public void createTable(String tableName, Set<String> familyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();) {
            HTableDescriptor htd = new HTableDescriptor(tn);
            for (String fc : familyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                htd.addFamily(hcd);
                hcd.setCompressionType(Compression.Algorithm.SNAPPY);
                hcd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
            }
            admin.createTable(htd,HbaseUitls.getSplitKeys());
        } catch (IOException e) {
            log.error("create table "+tableName+" failed！", e);
        }
    }
    public  boolean modifyTable(Object obj) {
        String tableName = "";
        try (Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            tableName = HbaseUitls.getORMTable(obj);
            if(admin.isTableAvailable(TableName.valueOf(tableName))) {
                List<Field> getFields = new LinkedList<>();
                List<Field> getFieldList = HBaseBeanUtil.getFieldList(obj.getClass(), getFields);
                Set<String> set = getFamilyColumn(getFieldList);
                // 修改表
                modifyTable(tableName, set);

           }

        }catch (Exception e){
            log.error("Hbase modify table is error:{}！",obj,e);
        }


        return true;
    }

    private Set<String> getFamilyColumn(List<Field> getFieldList) {
        Set<String> set = new HashSet<>(10);
        for (int i = 0; i < getFieldList.size(); i++) {
            Field field = getFieldList.get(i);
            if (!field.isAnnotationPresent(HbaseColumn.class)) {
                continue;
            }
            field.setAccessible(true);
            HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
            String family = orm.family();
            if (ROW_KEY.equalsIgnoreCase(family)) {
                continue;
            }
            set.add(family);
        }
        return set;
    }

    /**
     * @Descripton: 修改表属性
     * @param tableName
     */
    private void modifyTable(String tableName,Set<String> familyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();) {
            HTableDescriptor htd = new HTableDescriptor(tn);
            admin.disableTable(tn);
            for (String fc : familyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                    hcd.setTimeToLive(2147483647);
                    //hcd.setCompressionType(Compression.Algorithm.SNAPPY);
                    //hcd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
                htd.addFamily(hcd);
            }
            admin.modifyTable(tn,htd);
            admin.enableTable(tn);
        } catch (IOException e) {
            log.error("modify table "+tableName+" failed！", e);
        }
    }

    // 保存方法
    private boolean savePut(List<Put> puts, String tableName){

        if (StringUtils.isBlank(tableName)) {
            return false;
        }
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName)); Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            table.put(puts);
            return true;
        }catch (IOException e) {
            log.error("Hbase savePut is error,tableName:{}",tableName,e);
            return false;
        }
    }



    /**
     * @Descripton: 根据条件过滤查询
     * @param obj
     * @param param
     */
    public  List<Map<String,Object>> queryScanMap(Object obj, Map<String, String> param)throws Exception{
        List<Map<String,Object>> objs = new ArrayList<Map<String,Object>>();
        String tableName = HbaseUitls.getORMTable(obj);
        log.info("......................................EmrHBaseconnectionFactory queryScanMap tableName:{}",tableName);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }
        try (Table table = EmrHBaseconnectionFactory.connection.getTable(TableName.valueOf(tableName));Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            log.info("......................................EmrHBaseconnectionFactory queryScanMap connection:is success!");
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                return objs;
            }
            Scan scan = getScan(obj, param);
            log.info("......................................EmrHBaseconnectionFactory queryScanMap scan:{}",FastJsonUtils.toJSONString(scan));
            ResultScanner scanner = table.getScanner(scan);
            log.info("......................................EmrHBaseconnectionFactory queryScanMap scanner:{}",FastJsonUtils.toJSONString(scanner));
            for (Result result : scanner) {
                Map<String,Object> map = HBaseBeanUtil.resultToMap(result,obj);
                objs.add(map);
            }
            log.info("......................................EmrHBaseconnectionFactory queryScanMap objs:{}",FastJsonUtils.toJSONString(objs));
        } catch (Exception e) {
            log.error("Hbase  queryScan is error,tableName:{} ",tableName,e,e);
        }
        return objs;
    }

    private Scan getScan(Object obj, Map<String, String> param) {
        Scan scan = new Scan();
        for (Map.Entry<String, String> entry : param.entrySet()){
            Class<?> clazz = obj.getClass();
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (!field.isAnnotationPresent(HbaseColumn.class)) {
                    continue;
                }
                field.setAccessible(true);
                HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
                String family = orm.family();
                String qualifier = orm.qualifier();
                if(qualifier.equals(entry.getKey())){
                    Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(entry.getValue()));
                    scan.setFilter(filter);
                }
            }
        }
        return scan;
    }

    /**
     * @Descripton: 根据实体对象创建表结构
     * @param obj
     */
    public  boolean createTable(Object obj) {
        String tableName = "";
        try (Admin admin = EmrHBaseconnectionFactory.connection.getAdmin();){
            tableName = HbaseUitls.getORMTable(obj);
                // 表不存在，先获取family创建表
                if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                    synchronized(syncLock) {
                        createHbaseTable(obj, tableName, admin);
                    }
                }

        }catch (Exception e){
            log.error("Hbase create table is error:{}！",obj,e);
        }
        return true;
    }

    private void createHbaseTable(Object obj, String tableName, Admin admin) throws IOException {
        if (!admin.isTableAvailable(TableName.valueOf(tableName))){
            // 获取family, 创建表
            List<Field> getFields = new LinkedList<>();
            List<Field> getFieldList = HBaseBeanUtil.getFieldList(obj.getClass(), getFields);
            Set<String> set = getFamilyColumn(getFieldList);
            // 创建表
            createTable(tableName, set);
        }
    }

    /**
     * 删除指定时间戳内的数据
     * @param tableName
     * @param minTime
     * @param maxTime
     */
    public void deleteTimeRange(String tableName, Long minTime, Long maxTime) {
        Table table = null;
        Connection connection = null;
        try {
            Scan scan = new Scan();
            scan.setTimeRange(minTime, maxTime);
            connection = EmrHBaseconnectionFactory.connection;
            table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(scan);
            List<Delete> list = getDeleteList(rs);
            if (list.size() > 0) {
                table.delete(list);
            }
            log.info("................................Delete data within the specified timestamp success tabel:{},minTime:{},maxTime:{}",tableName,minTime,maxTime);
        } catch (Exception e) {
            log.info("................................Delete data within the specified timestamp error tabel:{},minTime:{},maxTime:{}",tableName,minTime,maxTime);
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.info("................................Delete data within the specified timestamp error tabel:{},minTime:{},maxTime:{}",tableName,minTime,maxTime);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    log.info("................................Delete data within the specified timestamp error tabel:{},minTime:{},maxTime:{}",tableName,minTime,maxTime);
                }
            }
        }

    }
    private List<Delete> getDeleteList(ResultScanner rs) {
        List<Delete> list = new ArrayList<>();
        try {
            for (Result r : rs) {
                Delete d = new Delete(r.getRow());
                list.add(d);
            }
        } finally {
            rs.close();
        }
        return list;
    }



}
