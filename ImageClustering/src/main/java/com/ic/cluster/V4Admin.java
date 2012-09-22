/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.cluster;

import com.ic.admin.HbaseAdmin;
import com.ic.downloader.GmmDownloader;
import com.ic.hbase.schema.ClusterDatabase;
import com.ic.hbase.schema.ImageClusterMap;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author phoenix
 */
public class V4Admin {

    public static void main(String[] args) {
        int level = 0;

        System.out.println("[CLEAN UP]:START");
        cleanupImageClusterMapTable();
        cleanup();
        System.out.println("[CLEAN UP]:COMPLETE");

        System.out.println("[LEVEL:0]:START");

        try {
            int pre1 = ToolRunner.run(new GmmDownloader(), args);
            int step1 = ToolRunner.run(new MatrixCreationI(level), args);
            int step2 = ToolRunner.run(new MatrixCreationII(level), args);
            int step3 = ToolRunner.run(new ClusterI(level), args);
            int step4 = ToolRunner.run(new ClusterII(level), args);
            
            level++;
        } catch (Exception ex) {
            Logger.getLogger(V4Admin.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("[LEVEL:0]:COMPLETE");


        level = 1;

        do {
            System.out.println("[LEVEL:" + level + "]:START");
            try {
                int stepI1 = ToolRunner.run(new MatrixCreationNewI(level), args);
                int stepI2 = ToolRunner.run(new MatrixCreationNewII(level), args);
                int stepI3 = ToolRunner.run(new ClusterNI(level), args);
                int stepI4 = ToolRunner.run(new ClusterNII(level), args);
                
                level++;
            } catch (Exception ex) {
                Logger.getLogger(V4Admin.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println("[LEVEL:" + level + "]:COMPLETE");
        } while (level < 5);
    }

    public static void cleanup() {
        try {
            HbaseAdmin hAdmin = new HbaseAdmin();
            hAdmin.disabletable("ClusterDatabase");
            hAdmin.deletetable("ClusterDatabase");
            ClusterDatabase.create();
        } catch (IOException ex) {
            Logger.getLogger(ClusterAdmin.class.getName()).log(Level.SEVERE, null, ex);
        }
    } 

    public static void cleanupImageClusterMapTable() {
        try {
            String tablename = "ImageClusterMap";
            HbaseAdmin hAdmin = new HbaseAdmin();
            hAdmin.disabletable(tablename);
            hAdmin.deletetable(tablename);
            ImageClusterMap.create();
        } catch (IOException ex) {
            Logger.getLogger(ClusterAdmin.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
