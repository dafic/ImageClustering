package com.ic.cluster;

import com.ic.admin.HbaseAdmin;
import com.ic.downloader.GmmDownloader;
import com.ic.downloader.ImageDownloader;
import com.ic.hbase.schema.ClusterDatabase;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author phoenix
 */
public class ClusterAdmin {

    public static void main(String[] args) throws Exception {
        int level = 0;
        int numOfNewCluster = -1;

        long startTime = System.currentTimeMillis();

        cleanup();

        int pre1 = ToolRunner.run(new GmmDownloader(), args);
        do {
            int step1 = ToolRunner.run(new SequenceFileCreation(level), args);
            int step2 = ToolRunner.run(new MatrixCreationI(), args);
            int step3 = ToolRunner.run(new MatrixCreationII(), args);
            int step4 = ToolRunner.run(new ClusterI(level), args);
            level++;
            numOfNewCluster = ToolRunner.run(new ClusterII(level), args);
            System.out.println("number of new cluster:" + numOfNewCluster);
        } while (level < 5);

        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken: " + ((endTime - startTime) / 1000));
        System.exit(1);
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
}