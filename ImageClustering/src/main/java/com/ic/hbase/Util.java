package com.ic.hbase;

import com.ic.common.ObjectAndByte;
import jMEF.MixtureModel;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
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
public class Util {

    public static void main(String[] args) {
        String imageName = "11.jpg";
        BufferedImage bufferedImage = Util.getBufferedImage(imageName);

    }

    public static byte[] getImageByte(String imageName) {

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.master", "127.0.0.1:60000");
            HTable table = new HTable(conf, "ImageDatabase");
            byte[] cf = Bytes.toBytes("gmm");
            byte[] cq = Bytes.toBytes("image");
            Get get = new Get(imageName.getBytes());
            get.addColumn(cf, cq);
            List<KeyValue> values = table.get(get).list();
            KeyValue kv = values.get(0);
            int length = kv.getValueLength();
            byte[] d = new byte[length];
            d = kv.getValue();
            return d;
        } catch (IOException ex) {
            Logger.getLogger(Util.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

    }

    public static BufferedImage getBufferedImage(String imageName) {
        BufferedImage image = null;
        try {
            byte[] d = Util.getImageByte(imageName);
            InputStream in = new ByteArrayInputStream(d);
            image = ImageIO.read(in);
            String imageType = imageName.substring(imageName.lastIndexOf(".") + 1);
            //ImageIO.write(image, imageType, new File(imageName));
        } catch (IOException ex) {
            Logger.getLogger(Util.class.getName()).log(Level.SEVERE, null, ex);
        }
        return image;
    }

    public static MixtureModel getMixtureModel(String gmmName) {
        MixtureModel m = null;
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.master", "127.0.0.1:60000");
            HTable table = new HTable(conf, "ClusterDatabase");
            byte[] cf = Bytes.toBytes("Cluster");
            byte[] cq = Bytes.toBytes("GMM");
            
            Get get = new Get(gmmName.getBytes());
            get.addColumn(cf, cq);
            
            List<KeyValue> kvList = table.get(get).list();
            if(kvList==null)
                return null;
            for (KeyValue kv : kvList) {
                if (Bytes.toString(kv.getQualifier()).equals("GMM")) {
                    try {
                        m = (MixtureModel) ObjectAndByte.byteArrayToObject(kv.getValue());
                    } catch (ClassNotFoundException ex) {
                        System.out.println("class not found");
                        ex.printStackTrace();
                    } catch (ArrayStoreException ase) {
                        System.out.println("array store exception:");
                    }
                    break;
                }
            }

        } catch (IOException ex) {
            System.out.println("IOException:" + ex.getMessage());
            Logger.getLogger(Util.class.getName()).log(Level.SEVERE, null, ex);

        }
        return m;
    }
}
