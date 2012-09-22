package com.ic.admin;

/*
 * Hbase administration basic tools.
 *
 * */
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseAdmin {

    public HbaseAdmin() {}

    public void addColumn(String tablename, String colunmnname) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.addColumn(tablename, new HColumnDescriptor(colunmnname));
        System.out.println("Added column : " + colunmnname + "to table " + tablename);
    }

    public void delColumn(String tablename, String colunmnname) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.deleteColumn(tablename, colunmnname);
        System.out.println("Deleted column : " + colunmnname + "from table " + tablename);
    }

    public void createTable(String tablename, String familyname) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);

        HTableDescriptor tabledescriptor = new HTableDescriptor(Bytes.toBytes(tablename));

        tabledescriptor.addFamily(new HColumnDescriptor(familyname));

        admin.createTable(tabledescriptor);

    }

    public void majorCompact(String mytable) throws IOException {
        //Create the required configuration.
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        //Instantiate a new client.
        HTable table = new HTable(conf, mytable);

        HBaseAdmin admin = new HBaseAdmin(conf);

        String tablename = table.toString();
        try {
            admin.majorCompact(tablename);
            System.out.println("Compaction done!");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void checkIfRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
        //Create the required configuration.
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        //Check if Hbase is running
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            System.err.println("Exception at " + e);
            System.exit(1);
        }
    }

    public void minorcompact(String trname) throws IOException, InterruptedException {
        //Create the required configuration.
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.compact(trname);
    }

    public void deletetable(String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.deleteTable(tablename);
    }

    public void disabletable(String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tablename);
    }

    public void enabletable(String tablename) throws IOException {
        //Create the required configuration.
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.enableTable(tablename);
    }

    public void flush(String trname) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(trname);
    }

    public ClusterStatus getclusterstatus() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.getClusterStatus();
    }

    public void printClusterDetails() throws IOException {
        ClusterStatus status = getclusterstatus();

        status.getServerInfo();
        Collection<HServerInfo> serverinfo = status.getServerInfo();
        for (HServerInfo s : serverinfo) {
            System.out.println("Servername " + s.getServerName());
            System.out.println("Hostname " + s.getHostname());
            System.out.println("Hostname:Port " + s.getHostnamePort());
            System.out.println("Info port" + s.getInfoPort());
            System.out.println("Server load " + s.getLoad().toString());
            System.out.println();
        }

        String version = status.getHBaseVersion();
        System.out.println("Version " + version);

        int regioncounts = status.getRegionsCount();
        System.out.println("Regioncounts :" + regioncounts);

        int servers = status.getServers();
        System.out.println("Servers :" + servers);

        double averageload = status.getAverageLoad();
        System.out.println("Average load: " + averageload);

        int deadservers = status.getDeadServers();
        System.out.println("Deadservers : " + deadservers);

        Collection<String> servernames = status.getDeadServerNames();
        for (String s : servernames) {
            System.out.println("Dead Servernames " + s);
        }

    }

    public boolean isTableAvailable(String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean result = admin.isTableAvailable(tablename);
        System.out.println("Table " + tablename + " available ?" + result);
        return result;
    }

    public boolean isTableEnabled(String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean result = admin.isTableEnabled(tablename);
        System.out.println("Table " + tablename + " enabled ?" + result);
        return result;
    }

    public boolean isTableDisabled(String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean result = admin.isTableDisabled(tablename);
        System.out.println("Table " + tablename + " disabled ?" + result);
        return result;
    }

    public boolean tableExists(String tablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean result = admin.tableExists(tablename);
        System.out.println("Table " + tablename + " exists ?" + result);
        return result;
    }

    public void shutdown() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        System.out.println("Shutting down..");
        admin.shutdown();
    }

    public void listTables() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.listTables();
    }

    @SuppressWarnings("deprecation")
    public void modifyColumn(String tablename, String columnname, String descriptor) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.modifyColumn(tablename, columnname, new HColumnDescriptor(descriptor));

    }

    public void modifyTable(String tablename, String newtablename) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.modifyTable(Bytes.toBytes(tablename), new HTableDescriptor(newtablename));

    }

    public void split(String tablename) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.split(tablename);
    }

    public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", AppConfig.HBASE_MASTER);
        HBaseAdmin administer = new HBaseAdmin(conf);
        System.out.println("Master running ? " + administer.isMasterRunning());
        return administer.isMasterRunning();
    }
}
