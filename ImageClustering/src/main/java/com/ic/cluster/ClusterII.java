package com.ic.cluster;

import com.ic.hbase.domain.ClusterObject;
import jMEF.MixtureModel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author phoenix
 */
public class ClusterII extends Configured implements Tool {

    private int level;

    public enum Counters {

        NUM_MERGES
    }

    public ClusterII(int level) {
        this.level = level;
    }

    public static class ClusterMapper extends Mapper<Text, Text, ImmutableBytesWritable, Writable> {

        private DoubleWritable outKey = new DoubleWritable();
        private Text outValue = new Text();
        private byte[] family = null;
        private byte[] gmmQualifier = null;
        private byte[] imageListQualifier = null;
        private byte[] constituentGMMQualifier = null;

        HTable imageToClusterMapTable;
        private byte[] imageClusterMapFamily=null;
        private byte[] imageClusterMapColQual=null;
        private byte[] imageClusterMapColQualMerged=null;
        
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            family = "Cluster".getBytes();
            gmmQualifier = "GMM".getBytes();
            imageListQualifier = "ImageList".getBytes();
            constituentGMMQualifier = "ConstituentGMM".getBytes();
            
            imageToClusterMapTable=new HTable(context.getConfiguration(),"ImageClusterMap");
            imageClusterMapFamily="Map".getBytes();
            imageClusterMapColQual="Cluster".getBytes();
            imageClusterMapColQualMerged="Merged".getBytes();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            int level = Integer.parseInt(context.getConfiguration().get("level"));
            System.out.println("level:" + level);
            //key format=> 0|1480001172,0|1506783281 i.e. gmmName1,gmmName2
            String keyString = key.toString();
            String imageList = "";
            String gmmList = "";
            List<ClusterObject> clusterObjectList=new ArrayList<ClusterObject>();
            
            
            if (keyString.contains(",")) {
                String[] constituentKeys = keyString.split(",");
                System.out.println("combined following:");
                for(String s:constituentKeys){
                    System.out.println(s);

                }
                int[] numImgInCluster=new int[constituentKeys.length];
                int totalImgInClusters=0;
                int index=0;
                for (String ck : constituentKeys) {
                    ClusterObject c = new ClusterObject(ck);
                    clusterObjectList.add(c);
                    imageList+=","+c.getImageListString();
                    numImgInCluster[index]=c.getImageList().length;
                    totalImgInClusters+=numImgInCluster[index];
                    index++;
                }
                System.out.println("total images="+totalImgInClusters);
                double[] weight=new double[constituentKeys.length];
                for(int i=0;i<constituentKeys.length;i++){
                    weight[i]=numImgInCluster[i]/(double)totalImgInClusters;
                }
                

//                MixtureModel resultMM = Mixture.mergeManyGMM(clusterObjectList, weight);
//                byte[] resultGMMBinaryData = ObjectAndByte.objectToByteArray(resultMM);
//                byte[] writableGMMBinaryData = Bytes.toBytes(Bytes.toStringBinary(resultGMMBinaryData));

                imageList = imageList.substring(1);//remove first comma (,) out
                gmmList = keyString;

                //byte[] rowkey = Bytes.toBytes(level+"|"+Math.abs(keyString.hashCode()));
                byte[] rowkey = Bytes.toBytes((level+1) + "|" + DigestUtils.md5Hex(keyString));

                Put put = new Put(rowkey);
//                put.add(family, gmmQualifier, writableGMMBinaryData);
                put.add(family, imageListQualifier, Bytes.toBytes(imageList));
                put.add(family, constituentGMMQualifier, Bytes.toBytes(gmmList));
                context.write(new ImmutableBytesWritable(rowkey), put);

                //ImageToClusterMapTable data insertion
                String[] gmmName=gmmList.split(",");
                Put putImageClusterMap1=new Put(gmmName[0].getBytes());
                putImageClusterMap1.add(imageClusterMapFamily, imageClusterMapColQual, rowkey);
                putImageClusterMap1.add(imageClusterMapFamily, imageClusterMapColQualMerged, "1".getBytes());
                
                Put putImageClusterMap2=new Put(gmmName[1].getBytes());
                putImageClusterMap2.add(imageClusterMapFamily, imageClusterMapColQual, rowkey);
                putImageClusterMap2.add(imageClusterMapFamily, imageClusterMapColQualMerged, "1".getBytes());
                
                imageToClusterMapTable.put(putImageClusterMap1);
                imageToClusterMapTable.put(putImageClusterMap2);
                
                System.out.println("list:" + imageList);
                context.getCounter(Counters.NUM_MERGES).increment(1);

            } else {
                //System.out.println(keyString);
                ClusterObject co1 = new ClusterObject(keyString);

                MixtureModel m = co1.getGmm();//MixtureModel m = Util.getMixtureModel(keyString);
                imageList = co1.getImageListString();
                gmmList = keyString;

                System.out.println("list:" + imageList);

                byte[] rowkey = Bytes.toBytes((level+1) + "|" + keyString.substring(keyString.indexOf("|")+1));
//
//                byte[] resultGMMBinaryData = ObjectAndByte.objectToByteArray(m);
//                byte[] writableGMMBinaryData = Bytes.toBytes(Bytes.toStringBinary(resultGMMBinaryData));

                Put put = new Put(rowkey);
//                put.add(family, gmmQualifier, writableGMMBinaryData);
                put.add(family, imageListQualifier, Bytes.toBytes(imageList));
                put.add(family, constituentGMMQualifier, Bytes.toBytes(gmmList));
                context.write(new ImmutableBytesWritable(rowkey), put);
                
                //ImageToClusterMapTable data insertion
                Put putImageClusterMap1=new Put(gmmList.getBytes());
                putImageClusterMap1.add(imageClusterMapFamily, imageClusterMapColQual, rowkey);
                putImageClusterMap1.add(imageClusterMapFamily, imageClusterMapColQualMerged, "0".getBytes());
                imageToClusterMapTable.put(putImageClusterMap1);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            imageToClusterMapTable.close();
        }
        
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("level", level + "");

        String inputFileName = "/cluster/lookup_"+level+".seq";
        String outputTable = "ClusterDatabase";

        Job job = new Job(conf, "Cluster New II");
        job.setJarByClass(ClusterII.class);
        job.setMapperClass(ClusterMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);


        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputFileName));

        job.setNumReduceTasks(0);


        if (job.waitForCompletion(true)) {
            Counter mergeCounter = job.getCounters().findCounter(Counters.NUM_MERGES);
            return (int) mergeCounter.getValue();
        } else {
            return 0;
        }
    }
}
