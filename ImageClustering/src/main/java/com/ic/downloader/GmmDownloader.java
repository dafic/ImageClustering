package com.ic.downloader;

/**
 *
 * @author phoenix
 */
import com.ic.common.ObjectAndByte;
import com.ic.common.Utility;
import jMEF.MixtureModel;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GmmDownloader extends Configured implements Tool {

    public static class DownloaderMapper extends Mapper<IntWritable, Text, BooleanWritable, Text> {

        private static Configuration conf;
        // This method is called on every node

        public void setup(Context jc) throws IOException {
            conf = jc.getConfiguration();
        }

        public void map(IntWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            HTable table = new HTable(conf, "ClusterDatabase");
            byte[] cf = Bytes.toBytes("Cluster");
            byte[] cq1 = Bytes.toBytes("GMM");
            byte[] cq2 = Bytes.toBytes("ImageList");

            String word = value.toString();

            BufferedReader reader = new BufferedReader(new StringReader(word));
            String uri;
            int i = key.get();
            int iprev = i;
            while ((uri = reader.readLine()) != null) {

                long startT = 0;
                long stopT = 0;
                startT = System.currentTimeMillis();

                try {
                    String type = "";
                    URLConnection conn;
                    // Attempt to download
                    context.progress();

                    try {
                        URL link = new URL(uri);
                        System.err.println("Downloading " + link.toString());
                        conn = link.openConnection();
                        conn.connect();
                        type = conn.getContentType();
                    } catch (Exception e) {
                        System.err.println("Connection error to gmm : " + uri);
                        continue;
                    }

                    if (type == null) {
                        continue;
                    } else {
                        //get byte[] of image
                        InputStream imageStream = conn.getInputStream();
                        byte dataGmm[] = Utility.readBytes(imageStream);
                        MixtureModel gmm = (MixtureModel) ObjectAndByte.byteArrayToObject(dataGmm);
                        byte[] resultGMMBinaryData = ObjectAndByte.objectToByteArray(gmm);
                        byte[] writableGMMBinaryData = Bytes.toBytes(Bytes.toStringBinary(resultGMMBinaryData));

                        String gmmName = Utility.getNameFromGmmURI(uri);

                        //put the data to HBase table
                        //String keyName = "0|" + Math.abs(gmmName.hashCode()) + "";
                        String keyName = "0|" + gmmName.substring(0,gmmName.indexOf("."));
                        Put put = new Put(keyName.getBytes());
                        put.add(cf, cq1, writableGMMBinaryData);
                        put.add(cf, cq2, gmmName.getBytes());
                        table.put(put);
                        System.out.println("inserting " + gmmName);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Error... probably cluster downtime");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }

                i++;

                // Emit success
                stopT = System.currentTimeMillis();
                float el = (float) (stopT - startT) / 1000.0f;
                System.err.println("> Took " + el + " seconds\n");
            }


            try {
                reader.close();
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // Setup configuration
        Configuration conf = new Configuration();
        conf.set("hbase.master", "127.0.0.1:60000");

        String inputFile = "/data/gmm/gmm_url";
        String outputFile = "/data/gmm/output";
        int nodes = 3;        //number of nodes to perform map task
        int result = 0;
        //try (FileSystem fileSystem = FileSystem.get(conf)) {
            FileSystem fileSystem = FileSystem.get(conf);
            Path outputpath = new Path(outputFile);
            if (fileSystem.exists(outputpath)) {
                fileSystem.delete(outputpath, true);
            }
            conf.setInt("downloader.nodes", nodes);

            Job job = new Job(conf, "downloader");
            job.setJarByClass(GmmDownloader.class);
            job.setMapperClass(DownloaderMapper.class);

            job.setInputFormatClass(DownloaderInputFormat.class);

            job.setMapOutputKeyClass(BooleanWritable.class);
            job.setMapOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, outputpath);

            DownloaderInputFormat.setInputPaths(job, new Path(inputFile));

            job.setNumReduceTasks(0);
            result = job.waitForCompletion(true) ? 0 : 1;
        //}
        return result;
    }

    public static void createDir(String path, Configuration conf) throws IOException {
        Path output_path = new Path(path);

        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(output_path)) {
            fs.mkdirs(output_path);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new GmmDownloader(), args);
        System.exit(res);
    }
}