package com.sdiread.statistic.hbase.base;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @author fyn
 * @since 2019/7/10
 */
@Component
@Slf4j
public class HconnectionFactory implements InitializingBean {

    @Value("${hbase.zookeeper.quorum}")
    private String zkQuorum;

    @Value("${hbase.master}")
    private String hBaseMaster;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String zkPort;

    @Value("${hbase.zookeeper.znode.parent}")
    private String znode;

    private static Configuration conf = HBaseConfiguration.create();

    public static Connection connection;

    @Override
    public void afterPropertiesSet(){
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zkPort);
        conf.set("zookeeper.znode.parent", znode);
        conf.set("hbase.master", hBaseMaster);
        conf.set("hbase.hregion.max.filesize","1024 * 1024 * 1024 * 1024 * 1024");
        try {
            connection = ConnectionFactory.createConnection(conf);
            log.info("hbase get connection success！");
        } catch (IOException e) {
            log.error("hbase get connection failed！",e,e);
        }
    }
}
