package com.ic.hbase.schema;

import com.ic.admin.AppConfig;
import com.ic.admin.HbaseAdmin;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author phoenix
 */
public class ClusterDatabase {
    final static String tableName=AppConfig.CLUSTER_DATABASE;
    final static String family1=AppConfig.CLUSTER_DATABASE_CF;
    
    /**
     * create the schema for the ClusterDatabase
     */
    public static boolean create(){
        try {
            HbaseAdmin admin=new HbaseAdmin();
            boolean tableExists = admin.tableExists(tableName);
            if(!tableExists){
                admin.createTable(tableName, family1);
                return true;
            }
            return false;
        } catch (IOException ex) {
            Logger.getLogger(ImageDatabase.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }
}
