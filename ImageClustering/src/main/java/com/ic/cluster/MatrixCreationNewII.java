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
public class MatrixCreationNewII extends Configured implements Tool {

    private int level;

    public MatrixCreationNewII() {
    }

    public MatrixCreationNewII(int level) {
        this.level = level;
    }

    public static class MatrixMapper extends Mapper<Text, Text, DoubleWritable, Text> {

        DoubleWritable outKey = new DoubleWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double keyDouble = Double.parseDouble(key.toString());
            outKey.set(keyDouble);
            context.write(outKey, value);
        }
    }

    public static class MatrixReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        String inputFileName = "/cluster/matrix_intermediate_"+level+".seq";
        String outputFileName = "/cluster/matrix_result_"+level+".seq";

        int result;
       // try{
            FileSystem fileSystem = FileSystem.get(conf); 
            Path outputpath = new Path(outputFileName);
            if (fileSystem.exists(outputpath)) {
                fileSystem.delete(outputpath, true);
            }

            Job job = new Job(conf, "Matrix Creation New II");
            job.setJarByClass(MatrixCreationNewII.class);

            job.setMapperClass(MatrixCreationNewII.MatrixMapper.class);
            job.setReducerClass(MatrixCreationNewII.MatrixReducer.class);

            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(inputFileName));
            FileOutputFormat.setOutputPath(job, outputpath);
            result = job.waitForCompletion(true) ? 0 : 1;
       // }
        return result;
    }
}
