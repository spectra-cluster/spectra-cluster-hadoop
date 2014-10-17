package uk.ac.ebi.pride.spectracluster.hadoop.hbase;
   /*
     * Compile and run with:
     * javac -cp `hbase classpath` TestHBase.java
     * java -cp `hbase classpath` TestHBase
     */

import org.apache.commons.dbcp.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.systemsbiology.remotecontrol.*;

import javax.sql.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.hbase.HBaseUtilities
 * eventuaklly thisd will be test code and ststic functions but for now it
 * is where I try to access HBase
 *
 * @author Steve Lewis
 * @date 30/09/13
 */
public class HBaseUtilities {
    public static final String ROOT_DIR_PROP = "hbase.rootdir";
    public static final String DISTRIBUTED_PROP = "hbase.cluster.distributed";
    public static final String QUORRUM_PROP = "hbase.zookeeper.quorum";

    // this is needed
    public static String DEFAULT_QUORUM_PROP = "hadoop-slave-001.ebi.ac.uk,hadoop-slave-008.ebi.ac.uk,hadoop-slave-027.ebi.ac.uk";


    private static Configuration gConfig;

    public static synchronized Configuration getConfig() {
        if (gConfig == null) {
            Configuration config = HBaseConfiguration.create();
            config.set(ROOT_DIR_PROP, "hdfs://hadoop-master-01.ebi.ac.uk:8020/hbase");

            config.set(QUORRUM_PROP, DEFAULT_QUORUM_PROP);
            config.set(DISTRIBUTED_PROP, "true");
            gConfig = config;
        }
        return gConfig;
    }

    public static String PHOENIX_DRIVER_CLASS = "com.salesforce.phoenix.jdbc.PhoenixDriver";

    public static String PHOENIX_JDBC_STRING = "jdbc:phoenix:";


    /**
     *
     * @return
     */
    public static DataSource getHBaseDataSource() {
        try {
            Class.forName(PHOENIX_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(e);
        }

        String user = RemoteUtilities.getUser();
        String password = RemoteUtilities.getPassword();
        BasicDataSource ret = new BasicDataSource();

        String connString = PHOENIX_JDBC_STRING +  DEFAULT_QUORUM_PROP ;

        ret.setUrl(connString);
        ret.setDriverClassName(PHOENIX_DRIVER_CLASS);
        ret.setUsername(user);
        ret.setPassword(password);

        try {
            Connection connection = ret.getConnection();
            connection.close();
        } catch (SQLException e) {
            throw new UnsupportedOperationException("cannot use connection " + connString + " as user " + user);

        }
        return ret;
    }


    public static HTable getTable(String name) {
        Configuration config = getConfig();
        try {
            return new HTable(config, name);
        } catch (IOException e) {
            throw new UnsupportedOperationException(e);
        }
    }


    public static String getUUIDId() {
        return UUID.randomUUID().toString();
    }


    public static Put buildPut(String s) {
        Put ret = new Put(s.getBytes());
        return ret;
    }

    public static void addValuesToFamily(Put p, String family, Map<String, String> items) {
        byte[] famliyBytes = family.getBytes();

        for (String key : items.keySet()) {
            String value = items.get(key);
            p.add(famliyBytes, key.getBytes(), value.getBytes());
        }
    }


    public static Get buildGet(String s) {
        Get ret = new Get(s.getBytes());
        return ret;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void runHbase(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            HTable table = new HTable(conf, "test-table");
            Put put = new Put(Bytes.toBytes("test-key"));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("value"));
            table.put(put);
        } finally {
            admin.close();
        }
    }

     public static void main(String[] args) {
         DataSource source = getHBaseDataSource();
         source = null;
     }
}
