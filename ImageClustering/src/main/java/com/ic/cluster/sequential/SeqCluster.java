 package com.ic.cluster.sequential;

import com.ic.common.ObjectAndByte;
import com.ic.common.Utility;
import jMEF.Mixture;
import jMEF.MixtureModel;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author phoenix
 */
public class SeqCluster {

    Configuration config;
    //Writer writer;
    //Reader reader;
    String fileName;
    Text key;
    Text value;

    public SeqCluster(String fileName, Configuration config) {
        this.config = config;
        this.fileName = fileName;
        this.key = new Text();
        this.value = new Text();
    }

    public void createFile() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "127.0.0.1:60000");
//        HTable table = new HTable(conf, "TestDatabase");
//        byte[] cf = Bytes.toBytes("Image");
//        byte[] cq1 = Bytes.toBytes("gmm");
        String line;
        String imageName;
        Writer writer = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(this.fileName), this.config);
            writer = SequenceFile.createWriter(fs, this.config, new Path(this.fileName), key.getClass(), value.getClass());

            System.out.print("Creating " + this.fileName + ": ");
            FileReader fr = new FileReader("gmm_url");
            BufferedReader reader = new BufferedReader(fr);
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            //for (int i = 0; i < 42; i++) {
                System.out.print(".");
//                if (i == 14 | i == 17 | i == 18 | i == 20 | i == 56 | i == 79 | i == 96) {
//                    continue;
//                }
                //imageName = i + ".JPG";
               
                imageName = line.substring(line.lastIndexOf("/") + 1);
                System.out.println("image=" + imageName);
                //line = "http://localhost/image_and_gmm/gmm/" + imageName + "_5D_004.mix";
                String type = "";
                URLConnection conn = null;
                try {
                    URL link = new URL(line);
                    //System.out.println("Downloading " + link.toString());
                    conn = link.openConnection();
                    conn.connect();
                    type = conn.getContentType();
                } catch (Exception e) {
                    System.err.println("\nError: Connection error to image : " + line);
                }
                if (type == null) {
                    System.err.println("\nError: Image Type is null");
                    continue;
                } else {
                    InputStream imageStream = null;
                    try {
                        imageStream = conn.getInputStream();
                        byte dataImage[] = Utility.readBytes(imageStream);
                        String out = Bytes.toStringBinary(dataImage);
                        //write to sequence file gmm.seq
                        key.set(imageName);
                        value.set(out);
                        writer.append(key, value);
//                        Put put = new Put(imageName.getBytes());
//                        put.add(cf, cq1, dataImage);
//
//                        table.put(put);
                    } catch (IOException ex) {
                        Logger.getLogger(SeqCluster.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        try {
                            imageStream.close();
                        } catch (IOException ex) {
                            Logger.getLogger(SeqCluster.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }

                }
            }
            System.out.println("\nCreating file: complete");
        } finally {
            IOUtils.closeStream(writer);
//            table.close();
        }
    }

    public void read() throws IOException {
        System.out.println("Reading records:");
        FileSystem fs = FileSystem.get(URI.create(this.fileName), config);
        Path path = new Path(this.fileName);
        Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, new Path(fileName), config);
            Writable tk = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            Writable tv = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), config);
            long position = reader.getPosition();
            while (reader.next(tk)) {
                System.out.print("[" + tk + "] ");
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        System.out.println("\nReading records: complete");
    }

    public void read2() throws IOException {
        System.out.println("Reading records:");
        FileSystem fs = FileSystem.get(URI.create(this.fileName), config);
        Path path = new Path(this.fileName);
        Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, new Path(fileName), config);
            Writable tk = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            Writable tv = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), config);
            long position = reader.getPosition();
            while (reader.next(tk, tv)) {
                String vWord[] = tv.toString().split("::");
                String name[] = vWord[0].split(",");

                System.out.print("[ ");
                for (String s : name) {
                    System.out.print(s + " ");
                }
                System.out.println("]");
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        System.out.println("Reading records: complete");
    }

    public void writeToHbase() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "127.0.0.1:60000");
        HTable table = new HTable(conf, "TestDatabase");  //table
        byte[] cf = Bytes.toBytes("list"); //column family
        byte[] cq = Bytes.toBytes("l");  //column qualifier

        FileSystem fs = FileSystem.get(URI.create(this.fileName), config);
        Path path = new Path(this.fileName);
        Reader reader = null;
        try {
            System.out.println("Writing to ClusterDatabase");
            reader = new SequenceFile.Reader(fs, new Path(fileName), config);
            Writable tk = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            Writable tv = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), config);
            long position = reader.getPosition();
            while (reader.next(tk, tv)) {
                String vWord[] = tv.toString().split("::");
                String nameIndex[] = tk.toString().split(":");
                Put put = new Put(nameIndex[1].getBytes());
                //Put put = new Put(tk.toString().getBytes());
                put.add(cf, cq, vWord[0].getBytes());
                table.put(put);
                System.out.println("[" + vWord[0] + "]");
                System.out.println();
            }
            System.out.println("Writing to HBase: complete");
        } finally {
            IOUtils.closeStream(reader);
            table.close();
        }
    }

    public void createMatrix(Map<Double, String> map, List<String> kList, List<MixtureModel> mList) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(this.fileName), config);
        Path path = new Path(this.fileName);
        Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, new Path(fileName), config);
            Writable k = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            Writable v = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), config);

            System.out.println("Creating matrix:");
            System.out.print("  loading gmm: ");
            while (reader.next(k, v)) {
                System.out.print(".");
                String gmmName = k.toString();
                byte[] gmmData = Bytes.toBytesBinary(v.toString());
                MixtureModel mm = null;

                mm = (MixtureModel) ObjectAndByte.byteArrayToObject(gmmData);

                kList.add(gmmName);
                mList.add(mm);
            }
            System.out.println("\n  loading gmm: complete");

            //create matrix
            System.out.print("  creating matrix: ");
            MixtureModel m1, m2;
            for (int i = 0; i < mList.size(); i++) {
                System.out.print(".");
                m1 = mList.get(i);
                for (int j = i + 1; j < mList.size(); j++) {
                    m2 = mList.get(j);
                    double distance = MixtureModel.KLDMC(m1, m2, 300);
                    String id = kList.get(i).toString().trim() + ":" + kList.get(j).toString().trim();
                    map.put(distance, id);
                }
            }
            System.out.println("\n  creating matrix: complete");
        } catch (ClassNotFoundException ex) {
            
        } finally {
            IOUtils.closeStream(reader);
        }

    }

    public void createMatrix2(Map<Double, String> map, List<String> kList, List<MixtureModel> mList) throws IOException {
        System.out.println("Creating matrix:");
        FileSystem fs = FileSystem.get(URI.create(this.fileName), config);
        Path path = new Path(this.fileName);
        Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, new Path(fileName), config);
            Writable k = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            Writable v = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), config);
            System.out.print("  loading gmm: ");
            while (reader.next(k, v)) {
                System.out.print(".");
                String vWord[] = v.toString().split("::");
//                System.out.println(vWord[1]);
                String name[] = vWord[0].split(",");
                byte[] gmmData = Bytes.toBytesBinary(vWord[1].toString());
                MixtureModel mm = null;
                try {
                    mm = (MixtureModel) ObjectAndByte.byteArrayToObject(gmmData);
                } catch (ClassNotFoundException ex) {
                    
                }
                //System.out.println(vWord[1]);
//                String name[]=vWord[0].split(",");
                kList.add(vWord[0]);
                mList.add(mm);
            }
            System.out.println("\n  loading gmm: complete");

            //create matrix
            System.out.print("  creating matrix: ");
            MixtureModel m1, m2;
            for (int i = 0; i < mList.size(); i++) {
                System.out.print(".");
                m1 = mList.get(i);
                for (int j = i + 1; j < mList.size(); j++) {
                    m2 = mList.get(j);
                    double distance = MixtureModel.KLDMC(m1, m2, 300);
                    String id = kList.get(i) + "-" + kList.get(j);
                    map.put(distance, id);
                }
            }
            System.out.println("\n  creating matrix: complete");
        } finally {
            IOUtils.closeStream(reader);
        }

    }

    public void cluster(Map<Double, String> map, List<String> kList, List<MixtureModel> mList) throws IOException {
        //Data structures
        Map<String, String> outMap = new HashMap<String,String>();
        Set<String> mergeSet = new HashSet<String>();
        Set<String> notMergeSet;
        double threshold = 10.25;

        //merge the closest10.2 clusters
        System.out.print("Clustering: ");
        notMergeSet = new HashSet<String>(kList);
        int level = 0, gIndex = 0, gNum = 0;
        for (double d : map.keySet()) {
            System.out.print(".");
            String[] c = map.get(d).split(":");
            if (d > threshold) {
                break;
            } else {
                if (mergeSet.contains(c[0])) {
                    continue;
                } else if (mergeSet.contains(c[1])) {
                    continue;
                } else {
                    //remove the cluster from not merged state
                    notMergeSet.remove(c[0]);
                    notMergeSet.remove(c[1]);

                    /*
                     * TODO merge two gmm to create a new one
                     */
                    int c0Index = kList.indexOf(c[0]);
                    int c1Index = kList.indexOf(c[0]);
                    key.set(c[0]);
                    double[] w = {0.5, 0.5};
                    MixtureModel m = Mixture.mergeGMM(mList.get(c0Index), mList.get(c1Index), w);
                    byte[] mData = ObjectAndByte.objectToByteArray(m);
                    String newKey = level + ":" + gIndex;
                    String newValue = (c[0] + "," + c[1]) + "::" + Bytes.toStringBinary(mData) + "::";
                    outMap.put(newKey, newValue);
                    mergeSet.add(c[0]);
                    mergeSet.add(c[1]);
                    gIndex++;
                }
            }
        }
        System.out.println("\nClustering: complete");
        //System.out.println("\nmerged are:" + mergeSet);

        Configuration conf = new Configuration();
        String newFileName = "result1.gmm";
        FileSystem fs1;
        Writer writer1 = null;

        try {
            Iterator iter = notMergeSet.iterator();
            Text tk = new Text();
            Text tv = new Text();

            fs1 = FileSystem.get(URI.create(newFileName), conf);
            writer1 = SequenceFile.createWriter(fs1, conf, new Path(newFileName), Text.class, Text.class);

            while (iter.hasNext()) {
                String kk = (String) iter.next();
                byte[] bb = ObjectAndByte.objectToByteArray(mList.get(kList.indexOf(kk)));
                String newKey = level + ":" + gIndex;
                String newValue = kk + "::" + Bytes.toStringBinary(bb) + "::";

                tk.set(newKey);
                tv.set(newValue);

                writer1.sync();
                writer1.append(tk, tv);
                gIndex++;
            }
            for (String s : outMap.keySet()) {
                tk.set(s);
                tv.set(outMap.get(s));
                writer1.append(tk, tv);
            }
        } finally {
            IOUtils.closeStream(writer1);
        }

    }

    public boolean cluster2(Map<Double, String> map, List<String> kList, List<MixtureModel> mList) throws IOException {
        //Data structures
        Map<String, String> outMap = new HashMap<String,String>();
        Set<String> mergeSet = new HashSet<String>();
        Set<String> notMergeSet;

        double threshold = 10;
        boolean flag = false;

        //System.out.println("map:" + map);
        //merge the closest clusters
        System.out.print("Clustering: ");
        notMergeSet = new HashSet<String>(kList);
        //System.out.println("not merge set:" + notMergeSet);
        //System.out.println("key list:" + kList);
        int level = 0, gIndex = 0, gNum = 0;
        for (double d : map.keySet()) {
            System.out.print(".");
            String abc = map.get(d);
            String[] c = map.get(d).split("-");

            if (d > threshold) {
                break;
            } else {
                if (mergeSet.contains(c[0])) {
                    continue;
                } else if (mergeSet.contains(c[1])) {
                    continue;
                } else {
                    //remove the cluster from not merged state
                    notMergeSet.remove(c[0]);
                    notMergeSet.remove(c[1]);


                    /*
                     * TODO merge two gmm to create a new one
                     */
                    int c0Index = kList.indexOf(c[0]);
                    int c1Index = kList.indexOf(c[0]);
                    //System.out.println("c0="+c[0]+" with"+c0Index+" c1index="+c1Index);
                    key.set(c[0]);
                    double[] w = {0.5, 0.5};
                    MixtureModel m = Mixture.mergeGMM(mList.get(c0Index), mList.get(c1Index), w);
                    byte[] mData = ObjectAndByte.objectToByteArray(m);
                    String newKey = level + ":" + gIndex;
                    String newValue = (c[0] + "," + c[1]) + "::" + Bytes.toStringBinary(mData) + "::";
                    outMap.put(newKey, newValue);
                    mergeSet.add(c[0]);
                    mergeSet.add(c[1]);
                    gIndex++;

                    flag = true;
                }
            }
        }
        System.out.println("\nClustering: complete");
        //System.out.println("\nmerged are:" + mergeSet);

        Configuration conf = new Configuration();
        String newFileName = "result2.gmm";
        FileSystem fs1;
        Writer writer1 = null;

        try {
            Iterator iter = notMergeSet.iterator();
            Text tk = new Text();
            Text tv = new Text();

            fs1 = FileSystem.get(URI.create(newFileName), conf);
            writer1 = SequenceFile.createWriter(fs1, conf, new Path(newFileName), Text.class, Text.class);

            while (iter.hasNext()) {
                String kk = (String) iter.next();
                byte[] bb = ObjectAndByte.objectToByteArray(mList.get(kList.indexOf(kk)));
                String newKey = level + ":" + gIndex;
                String newValue = kk + "::" + Bytes.toStringBinary(bb) + "::";

                tk.set(newKey);
                tv.set(newValue);

                writer1.sync();
                writer1.append(tk, tv);
                gIndex++;
            }
            for (String s : outMap.keySet()) {
                tk.set(s);
                tv.set(outMap.get(s));
                writer1.append(tk, tv);
            }
        } finally {
            IOUtils.closeStream(writer1);
        }
        return flag;
    }
}
