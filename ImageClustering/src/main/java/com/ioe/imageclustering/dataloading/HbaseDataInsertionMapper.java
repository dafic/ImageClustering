/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ioe.imageclustering.dataloading;

import com.ioe.imageclustering.common.util.hbase.HBaseImageUtil;
import com.ioe.imageclustering.common.util.hbase.HBaseUtils;
import com.ioe.imageclustering.gmm.utilites.ByteImageConversion;
import com.ioe.imageclustering.gmm.utilites.Image;
import com.ioe.imageclustering.gmm.utilites.ImageUtil;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;


import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author phoenix
 */
public class HbaseDataInsertionMapper extends Mapper<LongWritable, Text, BooleanWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String type = "";
            URLConnection conn = null;
            try {
                URL link = new URL(line);
                System.out.println("Downloading " + link.toString());
                conn = link.openConnection();
                conn.connect();
                type = conn.getContentType();
            } catch (Exception e) {
                System.err.println("Connection error to image : " + line);
            }
            if (type == null) {
                System.err.println("type is null");
                return;
            } else {
                //get image name and image type
                String imageName = line.substring(line.lastIndexOf("/") + 1);
                String imageType = imageName.substring(imageName.indexOf(".") + 1);

                //get byte[] of image
                InputStream imageStream = conn.getInputStream();
                byte dataImage[] = HBaseUtils.readBytes(imageStream);

                //construct bufferedImage from byte[] to generate GMM
                BufferedImage bufImage = ByteImageConversion.byteToBufferedImage(dataImage);
                if (bufImage == null) {
                    System.err.println("buffer image is null.");
                    return;
                }

                int bufferImageType = bufImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB : bufImage.getType();
                bufImage = ImageUtil.resizeImage(bufImage, bufferImageType);
                byte dataGMM[] = Image.getGMM(bufImage, 2);

                //load image and image type to HBase table
                HBaseImageUtil.insertImage(imageName, dataImage, imageType);

                //load image GMM to HBase table
                HBaseImageUtil.insertImageGMM(imageName, dataGMM);
                System.out.println("map complete for:" + imageName);

            }
            context.write(new BooleanWritable(true), new Text("map successful"));

        }
}