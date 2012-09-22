package com.ic.cluster;

import com.ic.common.ObjectAndByte;
import jMEF.MixtureModel;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;

/**
 *
 * @author phoenix
 */
public class MatrixCreationII extends Configured implements Tool {

    private int level;

    public MatrixCreationII() {
        this.level = 0;
    }

    public MatrixCreationII(int level) {
        this.level = level;
    }

    public static class MatrixMapper extends Mapper<Text, Text, DoubleWritable, Text> {

        private DoubleWritable outKey = new DoubleWritable();
        private Text outValue = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            /*
             * input key in form >> 0.jpg:1.jpg
             */
            /*
             * input value in form >> gmmValue1###gmmValue2
             */
            String[] splitImageName = key.toString().split(":");
            String[] splitGmm = value.toString().split("###");

            byte[] gmmBinary1 = Bytes.toBytesBinary(splitGmm[0]);
            byte[] gmmBinary2 = Bytes.toBytesBinary(splitGmm[1]);
            MixtureModel m1 = null;
            MixtureModel m2 = null;
            try {
                m1 = (MixtureModel) ObjectAndByte.byteArrayToObject(gmmBinary1);
                m2 = (MixtureModel) ObjectAndByte.byteArrayToObject(gmmBinary2);

                double distortion = MixtureModel.KLDMC(m1, m2, 100);
                outKey.set(distortion);
                outValue.set(key);
                context.write(outKey, outValue);

                System.out.println(splitGmm[0] + "," + splitGmm[1] + ":" + distortion);

            } catch (ClassNotFoundException ex) {
                System.out.println("class not found");
            } catch (ArrayStoreException ase) {
                System.out.println("array store exception:");
                System.out.println("gmm string is :\n" + splitGmm[0]);
                System.out.println("gmm string is :\n" + splitGmm[1]);
            }
        }
    }

    public static class MatrixReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        /*
         * input key in form >> 0.jpg
         */
        /*
         * input value in form >> 1.jpg:distance,2.jpg:distance
         */

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String imageName = key.toString();
            StringBuilder out = new StringBuilder();
            for (Text value : values) {
                out.append(value.toString() + ",");
            }
            System.out.println("key:" + key + " value:" + out);
            context.write(key, new Text(out.toString()));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        String inputFileName = "/cluster/matrix_intermediate_" + level + ".seq";
        String outputFileName = "/cluster/matrix_result_" + level + ".seq";

        int result;
        // try (FileSystem fileSystem = FileSystem.get(conf)) {
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputpath = new Path(outputFileName);
        if (fileSystem.exists(outputpath)) {
            fileSystem.delete(outputpath, true);
        }

        Job job = new Job(conf, "Matrix Creation II");
        job.setJarByClass(MatrixCreationII.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, outputpath);
        result = job.waitForCompletion(true) ? 0 : 1;
        //}
        return result;
    }
}
