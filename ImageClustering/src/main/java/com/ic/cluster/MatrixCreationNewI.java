package com.ic.cluster;

//import com.google.common.collect.Collections2;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * creates a sequence file that contains pairs of two gmm
 *
 * @author phoenix
 */
public class MatrixCreationNewI extends Configured implements Tool {

    private int level;

    public MatrixCreationNewI() {
    }

    public MatrixCreationNewI(int level) {
        this.level = level;
    }

    public static class MatrixMapper extends Mapper<DoubleWritable, Text, Text, Text> {

        static final IntWritable one = new IntWritable(1);
        Text keyOutput = new Text();
        Text ValueOutput = new Text();
        HTable imageToClusterMapTable;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            imageToClusterMapTable = new HTable(context.getConfiguration(), "ImageClusterMap");
        }

        @Override
        protected void map(DoubleWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println("key:" + key);
            System.out.println("value:" + value);
            String valueString = value.toString();
            int indexOfComma = valueString.lastIndexOf(",");
            if (indexOfComma > 0) {
                valueString = valueString.substring(0, indexOfComma);
            }
            String[] part = valueString.split(":");

            if (part[0].equals(part[1])) {
                return; //same cluster
            }
            String cluster1 = null;
            String cluster2 = null;

            Get get1 = new Get(part[0].getBytes());
            Get get2 = new Get(part[1].getBytes());

            get1.addColumn("Map".getBytes(), "Cluster".getBytes());
            get2.addColumn("Map".getBytes(), "Cluster".getBytes());

            Result result1 = imageToClusterMapTable.get(get1);
            Result result2 = imageToClusterMapTable.get(get2);
            boolean merged1 = false;
            boolean merged2 = false;
            try {
                List<KeyValue> list1 = result1.list();
                List<KeyValue> list2 = result2.list();
                for (KeyValue kv : list1) {
                    final String qualName = Bytes.toString(kv.getQualifier());
                    if (qualName.equals("Merged")) {
                        String qualValue = Bytes.toString(kv.getValue());
                        if (qualName.equals("1")) {
                            merged1 = true;
                        }
                    } else if (qualName.equals("Cluster")) {
                        cluster1 = Bytes.toString(kv.getValue());
                    }
                }
                for (KeyValue kv : list2) {
                    final String qualName = Bytes.toString(kv.getQualifier());
                    if (qualName.equals("Merged")) {
                        String qualValue = Bytes.toString(kv.getValue());
                        if (qualName.equals("1")) {
                            merged2 = true;
                        }
                    } else if (qualName.equals("Cluster")) {
                        cluster2 = Bytes.toString(kv.getValue());
                    }
                }

                if (!merged1 & !merged2) {
                    keyOutput.set(cluster1);
                    ValueOutput.set(cluster2 + ":" + key);
                } else if (merged1 & !merged2) {
                    keyOutput.set(cluster1);
                    ValueOutput.set(cluster2 + ":" + key);
                } else if (!merged1 & merged2) {
                    keyOutput.set(cluster2);
                    ValueOutput.set(cluster1 + ":" + key);
                }
                context.write(keyOutput, ValueOutput);
            } catch (NullPointerException npe) {
                System.out.println("Following doesnot exist.");
                System.out.println(part[0]);
                System.out.println(part[1]);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            imageToClusterMapTable.close();
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {

//        DoubleWritable outKey = new DoubleWritable();
//        Text outValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            DoubleWritable outKey = new DoubleWritable();
            Text outValue = new Text();

            System.out.println("\n\nkey: " + key);
            Map<String, Double> map = new HashMap<String, Double>();
            for (Text t : values) {
                String inputKeyString = key.toString();
                String inputValueString = t.toString();
                String[] split = inputValueString.split(":");
                if(split[1].contains("NaN")) continue;
                double dist = Double.parseDouble(split[1]);

                List<String> outputKeyList = new ArrayList<String>();
                outputKeyList.add(inputKeyString);
                outputKeyList.add(split[0]);
                Collections.sort(outputKeyList);

                String mapKey = outputKeyList.get(0) + ":" + outputKeyList.get(1);
//                System.out.println("map:"+map);
                if (map.containsKey(mapKey)) {

                    double max = dist;
                    double val = 0;
                    try {
                        val = map.get(mapKey);
                        max = val > dist ? val : dist;
                    } catch (NullPointerException npe) {
                        System.out.println("val=" + val + " dist=" + dist);
                    }
                    map.put(mapKey, max);
                } else {
                    map.put(mapKey, dist);
                }
            }

            NumberFormat nf = NumberFormat.getInstance();
            nf.setMinimumIntegerDigits(3);
            nf.setMaximumIntegerDigits(3);
            nf.setMaximumFractionDigits(8);
            nf.setMinimumFractionDigits(8);


            for (String s : map.keySet()) {
                double d = map.get(s);
                outKey.set(d);
                outValue.set(s);
                System.out.println("writing key:" + outKey + " value:" + outValue);
//                context.write(new DoubleWritable(map.get(s)), outValue);
                s = nf.format(d);
                if(!s.contains("NaN"))
                context.write(new Text(s), outValue);
//                context.write(outKey, outValue);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        String inputFileName = "/cluster/matrix_result_" + (level - 1) + ".seq";
        String outputFileName = "/cluster/matrix_intermediate_" + level + ".seq";

        int result;
        //try (FileSystem fileSystem = FileSystem.get(conf)) {
            FileSystem fileSystem = FileSystem.get(conf);
            Path outputpath = new Path(outputFileName);
            if (fileSystem.exists(outputpath)) {
                fileSystem.delete(outputpath, true);
            }

            Job job = new Job(conf, "Matrix Creation New I");
            job.setJarByClass(MatrixCreationNewI.class);

            job.setMapperClass(MatrixCreationNewI.MatrixMapper.class);
            job.setReducerClass(MatrixCreationNewI.MatrixReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(inputFileName));
            FileOutputFormat.setOutputPath(job, outputpath);
            result = job.waitForCompletion(true) ? 0 : 1;
        //}
        return result;
    }
}
