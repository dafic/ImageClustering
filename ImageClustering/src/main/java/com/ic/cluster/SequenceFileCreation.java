package com.ic.cluster;

import com.ic.common.ObjectAndByte;
import jMEF.MixtureModel;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;

/**
 *
 * @author phoenix
 */
public class SequenceFileCreation extends Configured implements Tool {
    private int level;
    public SequenceFileCreation(int level) {
        this.level=level;
    }
    

    public static class GmmMapper extends TableMapper<ImmutableBytesWritable, Text> {

        @Override
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            //String level=context.getConfiguration().get("level");
            //System.out.println("level:"+level);
            String value = null;
            try {
                for (KeyValue kv : columns.list()) {
                    byte[] gmmData = kv.getValue();
                    String gmmString = Bytes.toStringBinary(gmmData);

                   // /* just for checking that gmm is correctly constructed
//                    MixtureModel m = null;
//                    m = (MixtureModel) ObjectAndByte.byteArrayToObject(Bytes.toBytesBinary(gmmString));
//                    System.out.println("m.size:" + m.size);
                   // */
                    
                    context.write(key, new Text(gmmString));

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("level:"+level);
        Configuration conf = new Configuration();
        conf.set("level", level+"");
        String table = "ClusterDatabase";
        String seqFileName="/cluster/gmm.seq";
        
        Scan scan = new Scan();
        scan.setStartRow((level+"|").getBytes());
        scan.setStopRow(Bytes.add((level+"|").getBytes(), Bytes.toBytes(Long.MAX_VALUE)));
        scan.addColumn("Cluster".getBytes(), "GMM".getBytes());
        int result;
        //try (FileSystem fileSystem = FileSystem.get(conf)) {
            FileSystem fileSystem = FileSystem.get(conf);
            Path outputpath = new Path(seqFileName);
            if (fileSystem.exists(outputpath)) {
                fileSystem.delete(outputpath, true);
            }

            Job job = new Job(conf, "Gmm sequence file creation From HBASE");
            job.setJarByClass(SequenceFileCreation.class);
            TableMapReduceUtil.initTableMapperJob(table, scan, GmmMapper.class, ImmutableBytesWritable.class, Text.class, job);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setNumReduceTasks(0);
            FileOutputFormat.setOutputPath(job, outputpath);
            result = job.waitForCompletion(true) ? 0 : 1;
        //}
        return result;
    }
}
