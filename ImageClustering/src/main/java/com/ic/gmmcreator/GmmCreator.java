/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.gmmcreator;

import com.ic.common.ByteImageConversion;
import com.ic.common.Image;
import com.ic.common.ImageUtil;
import com.ic.common.ObjectAndByte;
import jMEF.MixtureModel;
import java.awt.image.BufferedImage;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author phoenix
 */
public class GmmCreator {

    public static class GmmMapper extends TableMapper<ImmutableBytesWritable, Text> {

        @Override
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            String value = null;
            try {
                for (KeyValue kv : columns.list()) {
                    //BufferedImage image=(BufferedImage) ObjectAndByte.byteArrayToObject(kv.getValue());
                    //construct bufferedImage from byte[] to generate GMM
                    byte[] imageData = kv.getValue();
                    BufferedImage bufImage = ByteImageConversion.byteToBufferedImage(imageData);
                    if (bufImage == null) {
                        System.err.println("buffer image is null.");
                        return;
                    }

                    int bufferImageType = bufImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB : bufImage.getType();
                    bufImage = ImageUtil.resizeImage(bufImage, bufferImageType);
                    MixtureModel m = Image.getGMM(bufImage, 2);
                    byte[] dataGmm = ObjectAndByte.objectToByteArray(m);
                    System.out.println("m.size:" + m.size);
                    context.write(key, new Text(Bytes.toStringBinary(dataGmm)));

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String table="ImageDatabase";
        
        Scan scan=new Scan();
        scan.addColumn("Image".getBytes(), "split0".getBytes());
        
        Job job = new Job(conf, "Gmm sequence file creation from Hbase Image Database");
        job.setJarByClass(GmmCreator.class);
        TableMapReduceUtil.initTableMapperJob(table, scan, GmmMapper.class,ImmutableBytesWritable.class, Text.class, job);

        //job.setMapperClass(GmmMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Text.class);
        //job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path("seqHbase.gmm"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
