
package com.ic.admin;

import com.ic.common.Directory;
import com.ic.downloader.GmmDownloader;
import com.ic.downloader.ImageDownloader;
import com.ic.hbase.schema.ClusterDatabase;
import com.ic.hbase.schema.ImageClusterMap;
import com.ic.hbase.schema.ImageDatabase;
import com.ic.hbase.schema.MetaDatabase;
import java.io.IOException;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author phoenix
 */
public class Main {

    public static void main(String[] args) throws Exception {
        
        /*<for first run only>*/
        //create table schema
//        createTables();
        //insert images list to HDFS
//        insertImageList();
        //insert Gmm list to HDFS
//        insertGmmList();
        //insert the images to ImageDatabase
//        int result1 = ToolRunner.run(new ImageDownloader(), args);
        //insert the gmm to ImageDatabase
        //int result2 = ToolRunner.run(new GmmDownloader(), args);
        /*</for first run only>*/

    }
    
    public static void createTables(){
        /*create the schema for the tables: ImageDatabase, ClusterDatabase, MetaDatabase*/
        if(ImageDatabase.create()) System.out.println("ImageDatabase created.");
        if(ClusterDatabase.create()) System.out.println("ClusterDatabase created.");
        if(MetaDatabase.create()) System.out.println("MetaDatabase created.");
        if(ImageClusterMap.create()) System.out.println("ImageClusterMap created.");
    }
    
    public static void insertImageList(){
        try {
            String prefix = AppConfig.LOCALHOST+"/image_and_gmm/dataset/";
            String outputFile = "image_url";
            Directory.createList(AppConfig.IMAGE_LOCATION, prefix, outputFile);
            //import the list to HDFS
            HDFSClient client = new HDFSClient();
            client.mkdir("/data/image");
            client.copyFromLocal(outputFile, "/data/image");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
    public static void insertGmmList(){
        try {
            String prefix = AppConfig.LOCALHOST+"/image_and_gmm/gmm/";
            String outputFile = "gmm_url";
            Directory.createList(AppConfig.GMM_LOCATION, prefix, outputFile);
            //import the list to HDFS
            HDFSClient client = new HDFSClient();
            client.mkdir("/data/gmm");
            client.copyFromLocal(outputFile, "/data/gmm");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
