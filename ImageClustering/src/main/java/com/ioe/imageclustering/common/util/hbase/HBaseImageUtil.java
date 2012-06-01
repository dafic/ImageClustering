/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ioe.imageclustering.common.util.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 *
 * @author phoenix
 */
public class HBaseImageUtil {

    public static void insertImage(String imageName, byte[] imageData, String imageType) throws IOException {
        String tableName = "ImageDatabase";
        String colFamily1 = "Image";
        String colFamily2 = "ImageInfo";
        String colQual1 = "split";
        String colQual2 = "imageType";
        System.out.println("image loading started");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "127.0.0.1:60000");
        HTable table = new HTable(conf, tableName);
        Put put = new Put(imageName.getBytes());
        
        int limit=3*1024*1024; 
        int n = (int) (imageData.length / limit);
        if (n == 0) {
            put.add(colFamily1.getBytes(), (colQual1 + n + "").getBytes(), imageData);
        } else {
            byte[] outputBytes = new byte[limit];
            for (int i = 0; i < n; i++) {
                System.arraycopy(imageData, limit*i, outputBytes, 0, limit);
                put.add(colFamily1.getBytes(), (colQual1+(i)).getBytes(), imageData);
            }
            System.arraycopy(imageData, limit*n, outputBytes, 0, imageData.length-limit*n);
            put.add(colFamily1.getBytes(), (colQual1+n).getBytes(), imageData);

        }
        
        put.add(colFamily2.getBytes(), colQual2.getBytes(), imageType.getBytes());

        table.put(put);

        table.close();
        System.out.println("loading image: " + imageName + " |Successful");
    }

    public static void insertImageGMM(String imageName, byte[] imageGMMData) throws IOException {
        String tableName = "ImageDatabase";
        String colFamily2 = "ImageInfo";
        String colQual1 = "gmm";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "127.0.0.1:60000");
        HTable table = new HTable(conf, tableName);
        Put put = new Put(imageName.getBytes());
        put.add(colFamily2.getBytes(), colQual1.getBytes(), imageGMMData);

        table.put(put);
        table.close();
        System.out.println("loading image: " + imageName + " |Successful");
    }
}
