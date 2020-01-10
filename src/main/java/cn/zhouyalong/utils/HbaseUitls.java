package com.sdiread.statistic.hbase.base;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import lombok.extern.slf4j.Slf4j;

/**
 * @author fyn
 * @since 2019/7/10
 **/
@Slf4j
public class HbaseUitls {
    private final static String TABLE_CONNECTOR = "_";
    /**
     * 每个表默认指定20个分区，每个分区最大值为1024T
     * @return
     */
    public static byte[][] getSplitKeys() {
        String[] keys = new String[] { "1|", "2|", "3|", "4|", "5|",
                "6|", "7|", "8|","9|", "10|", "11|", "12|", "13|",
                "14|", "15|", "16|" , "17|", "18|", "19|", "20|"};
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIterator = rows.iterator();
        int i=0;
        while (rowKeyIterator.hasNext()) {
            byte[] tempRow = rowKeyIterator.next();
            rowKeyIterator.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    public static String getRowKey(String rId,Long tm){
        Long revertTm =  Long.MAX_VALUE - tm;
        String rowKey = ConsistentHashingUtils.getServer(rId)+":"+rId+":"+revertTm;
        log.info("...........................getRowKey;rid:{},tm:{}:rowKey:{}",rId,tm,rowKey);
        return rowKey;
    }

    // 获取tableName
    public static String getORMTable(Object obj) {
        HbaseTable table = obj.getClass().getAnnotation(HbaseTable.class);
        String profile  = SpringContextUtil.getActiveProfile();
        return table.tableName() + TABLE_CONNECTOR + profile;
    }
}
