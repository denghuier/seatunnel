package org.apache.seatunnel.connectors.seatunnel.hbase.client;

import com.alibaba.dcm.DnsCacheManipulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Map;


@Slf4j
public class HbaseClient {

    private Connection connection;


    private HbaseClient(Connection connection){
        this.connection=connection;
    }
//    public static HbaseClient createInstance(Map<String,String> hosts,String zk){
//       return createInstance(hosts,zk);
//    }

    public static HbaseClient createInstance(Map<String,String> hosts,String quorum){
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        // 如果是集群 则主机名用逗号分隔
        configuration.set("hbase.zookeeper.quorum", quorum);
        for (String key : hosts.keySet()) {
            DnsCacheManipulator.setDnsCache(key, hosts.get(key));
        }
        Connection connection=null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HbaseClient(connection);
    }

    public  ResultScanner getScanner(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void close() {
       try {
           connection.close();
       }catch (IOException e) {
           log.warn("close hbase connection error", e);
       }
    }

}
