/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.hbase.domain;

import com.ic.common.ObjectAndByte;
import com.ic.hbase.Util;
import jMEF.MixtureModel;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author phoenix
 */
public class ClusterObject {

    private String key;
    private String[] imageList;
    private String imageListString;
    private MixtureModel gmm;
    private String[] constituentGMM;

    public String[] getConstituentGMM() {
        return constituentGMM;
    }

    public MixtureModel getGmm() {
        return gmm;
    }

    public String[] getImageList() {
        return imageList;
    }

    public String getImageListString() {
        return imageListString;
    }

    public void setImageListString(String imageListString) {
        this.imageListString = imageListString;
    }

    public ClusterObject(String key) {
        this.key = key;
        MixtureModel m = null;
        imageList = null;
        constituentGMM = null;

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.master", "127.0.0.1:60000");
            HTable table = new HTable(conf, "ClusterDatabase");
            byte[] cf = Bytes.toBytes("Cluster");
            byte[] cq1 = Bytes.toBytes("GMM");
            byte[] cq2 = Bytes.toBytes("ImageList");
            byte[] cq3 = Bytes.toBytes("ConstituentGMM");

            Get get = new Get(key.getBytes());
            //get.addColumn(cf, cq1);
            //get.addColumn(cf, cq2);
            //get.addColumn(cf, cq1);

            List<KeyValue> kvList = table.get(get).list();
            if (kvList == null) {
                System.out.println("no such data");
            }
            for (KeyValue kv : kvList) {
                //System.out.println("key qual:"+Bytes.toString(kv.getQualifier()));
                if (Bytes.toString(kv.getQualifier()).equals("GMM")) {
                    try {
                        byte[] gmmData = kv.getValue();
                        String gmmString = Bytes.toStringBinary(gmmData);
                        gmm = (MixtureModel) ObjectAndByte.byteArrayToObject(Bytes.toBytesBinary(gmmString));
                        // gmm = (MixtureModel) ObjectAndByte.byteArrayToObject(kv.getValue());
                        //System.out.println("gmm.size inside:"+gmm.size);
                    } catch (ClassNotFoundException ex) {
                        System.out.println("class not found");
                        ex.printStackTrace();
                    } catch (ArrayStoreException ase) {
                        System.out.println("array store exception:");
                    }
                } else if (Bytes.toString(kv.getQualifier()).equals("ImageList")) {
                    String list = Bytes.toString(kv.getValue());
                    this.imageListString = list;
                    if (list.contains(",")) {
                        imageList = list.split(",");
                    } else {
                        imageList = new String[1];
                        imageList[0] = list;
                    }
                    //System.out.println("list:"+list);
                } else if (Bytes.toString(kv.getQualifier()).equals("ConstituentGMM")) {
                    String consGMM = Bytes.toString(kv.getValue());
                    //System.out.println("cons GMM:"+consGMM);
                }
            }
        } catch (IOException ex) {
            System.out.println("IOException:" + ex.getMessage());
            Logger.getLogger(Util.class.getName()).log(Level.SEVERE, null, ex);

        }

    }
}
