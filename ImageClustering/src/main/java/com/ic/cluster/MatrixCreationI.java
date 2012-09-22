package com.ic.cluster;

import com.ic.common.ObjectAndByte;
import jMEF.MixtureModel;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * creates a sequence file that contains pairs of two gmm
 *
 * @author phoenix
 */
public class MatrixCreationI extends Configured implements Tool {

    private int level;

    public MatrixCreationI() {
    }

    public MatrixCreationI(int level) {
        this.level = level;
    }

    public static class MatrixMapper extends TableMapper<IntWritable, Text> {

        static final IntWritable one = new IntWritable(1);

//        @Override
//        protected void map(ImmutableBytesWritable key, Text value, Context context) throws IOException, InterruptedException {
//            Text combinedKeyValue = new Text();
//            //the structure is key###value
//            combinedKeyValue.set(Bytes.toString(key.get()) + "###" + value.toString());
//            context.write(one, combinedKeyValue);
//        }
        @Override
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {

            Text combinedKeyValue = new Text();
            //the structure is key###value
            String value = null;
            try {
                for (KeyValue kv : columns.list()) {
                    byte[] gmmData = kv.getValue();
                    String gmmString = Bytes.toStringBinary(gmmData);

                    // /* just for checking that gmm is correctly constructed
                    MixtureModel m = null;
                    m = (MixtureModel) ObjectAndByte.byteArrayToObject(Bytes.toBytesBinary(gmmString));
                    System.out.println("m.size:" + m.size);
                    // */
                    combinedKeyValue.set(Bytes.toString(key.get()) + "###" + gmmString);
                    context.write(one, combinedKeyValue);
//                    context.write(key, new Text(gmmString));

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MatrixReducer extends Reducer<IntWritable, Text, Text, Text> {
        /*
         * output key in form >> 0.jpg:1.jpg
         */
        /*
         * output value in form >> gmmValue1###gmmValue2
         */

        List<String> keyList = new ArrayList<String>();
        List<String> valueList = new ArrayList<String>();
        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String kv = value.toString();
                //kv is of form key###value
                int indexSeparator = kv.indexOf("###");
                String keyOnly = kv.substring(0, indexSeparator);
                String gmmString = kv.substring(indexSeparator + 3);

                /*
                 * just for testing that casting is error free
                 */
                byte[] gmmBinary = Bytes.toBytesBinary(gmmString);
                MixtureModel m = null;
                try {
                    m = (MixtureModel) ObjectAndByte.byteArrayToObject(gmmBinary);
                    //System.out.println("m.size:" + m.size);
                    System.out.println("Gmm in good state:" + keyOnly);
                    keyList.add(keyOnly);
                    valueList.add(gmmString);
                } catch (ClassNotFoundException ex) {
                    ex.printStackTrace();
                } catch (ArrayStoreException ase) {
                    System.out.println("array store exception:");
                    System.out.println("gmm corrupted :" + keyOnly);
                }
                /*
                 * testing ends here
                 */
            }
            int numOfGmm = keyList.size();
            for (int i = 0; i < numOfGmm; i++) {
                for (int j = i + 1; j < numOfGmm; j++) {
                    outKey.set(keyList.get(i) + ":" + keyList.get(j));
                    outValue.set(valueList.get(i) + "###" + valueList.get(j));
                    context.write(outKey, outValue);
                }
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        //String inputFileName = "/cluster/gmm.seq";
        String outputFileName = "/cluster/matrix_intermediate_"+level+".seq";

        int result;
        System.out.println("level:" + level);
        conf.set("level", level + "");
        String table = "ClusterDatabase";
        //String seqFileName = "/cluster/gmm.seq";

        Scan scan = new Scan();
        scan.setStartRow((level + "|").getBytes());
        scan.setStopRow(Bytes.add((level + "|").getBytes(), Bytes.toBytes("ffffffffffffffffffffffffffffffff")));
        scan.addColumn("Cluster".getBytes(), "GMM".getBytes());

        //try (FileSystem fileSystem = FileSystem.get(conf)) {
            FileSystem fileSystem = FileSystem.get(conf);
            Path outputpath = new Path(outputFileName);
            if (fileSystem.exists(outputpath)) {
                fileSystem.delete(outputpath, true);
            }

            Job job = new Job(conf, "Matrix Creation I From HBase");
            job.setJarByClass(MatrixCreationI.class);
            TableMapReduceUtil.initTableMapperJob(table, scan, MatrixMapper.class, IntWritable.class, Text.class, job);
            job.setReducerClass(MatrixReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//            job.setInputFormatClass(TableInputFormat.class);
            //job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setNumReduceTasks(1);
            //FileInputFormat.addInputPath(job, new Path(inputFileName + "/part*"));
            FileOutputFormat.setOutputPath(job, outputpath);
            result = job.waitForCompletion(true) ? 0 : 1;
        //}
        return result;
    }
}
