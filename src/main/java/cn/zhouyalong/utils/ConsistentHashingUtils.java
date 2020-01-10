package com.sdiread.statistic.hbase.base;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author fyn
 * @since 2019/5/13
 **/
@Slf4j
public class ConsistentHashingUtils {
    /**
     * 虚拟节点的数目一个真实结点对应51个虚拟节点
     */
    private static final int VIRTUAL_NODES = 1024;

    private static final int MAX_HASH_VALUE = 2147262909;

    private static final String SEPARATE_CHARACTER = "&&";

    private static final String VIRTUAL_NODE_SEPARATE_CHARACTER = "&&VN";
    /**
     * 待添加入Hash环的region列表
     */
    private static String[] regionServers = {"1", "2", "3", "4", "5","6", "7", "8", "9", "10", "11", "12","13", "14", "15", "16", "17","18", "19", "20"};
    /**
     * 真实结点列表,考虑到服务器上线、下线的场景，即添加、删除的场景会比较频繁，这里使用LinkedList会更好
     */
    private static List<String> realNodes = new LinkedList<>();
    /**
     * 虚拟节点，key表示虚拟节点的hash值，value表示虚拟节点的名称
     */
    private static ConcurrentSkipListMap<Integer, String> virtualNodes =
            new ConcurrentSkipListMap<>();

    static {
        // 先把原始的服务器添加到真实结点列表中
        Collections.addAll(realNodes, regionServers);

        // 再添加虚拟节点，遍历LinkedList使用foreach循环效率会比较高
        for (String str : realNodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualNodeName = str + VIRTUAL_NODE_SEPARATE_CHARACTER + String.valueOf(i);
                int hash = getHash(virtualNodeName);
                virtualNodes.put(hash, virtualNodeName);
            }
        }
    }

    /**
     * 使用FNV1_32_HASH算法计算服务器的Hash值,这里不使用重写hashCode的方法，最终效果没区别
     */
    private static int getHash(String str) {
        final int p = 16777619;
        int hash = (int)2166136261L;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        // 如果算出来的值为负数则取其绝对值
        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }

    /**
     * 得到应当路由到的regionServer
     */
    public static String getServer(String node) {
        // 得到带路由的结点的Hash值
        int hash = getHash(node);
        if(hash > MAX_HASH_VALUE){
            hash = getHash(String.valueOf(hash));
        }
        // 得到大于该Hash值的所有Map
        SortedMap<Integer, String> subMap =
                virtualNodes.tailMap(hash);
        if(Objects.isNull(subMap) || subMap.size()==0){
            subMap = virtualNodes.tailMap(getHash(String.valueOf(hash)));
        }
        try {
            // 第一个Key就是顺时针过去离node最近的那个结点
            Integer i = subMap.firstKey();
            // 返回对应的虚拟节点名称，这里字符串稍微截取一下
            String virtualNode = subMap.get(i);
            return virtualNode.substring(0, virtualNode.indexOf(SEPARATE_CHARACTER));
        } catch (Exception e){
            log.error("SubMap is empty, subMap size = {}, virtualNodes size = {}, hash code = {}", subMap.size(), virtualNodes.size(), hash, e);
            throw e;
        }
    }

    public static void main(String[] args) {
//        byte[][] splitKeys =new byte[20][];
//        String[] regionServers = {"1", "2", "3", "4", "5","6", "7", "8", "9", "10", "11", "12","13", "14", "15", "16", "17","18", "19", "20"};
//        for(int i= 0 ;i<regionServers.length;i++){
//            splitKeys[i] = regionServers[i].getBytes();
//        }

        String server = getServer("bJebZFnwE2ACCQvi9QBUc5ywaBDB0gSq");
    }
}