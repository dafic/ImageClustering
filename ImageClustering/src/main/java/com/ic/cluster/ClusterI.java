package com.ic.cluster;

import com.ic.admin.AppConfig;
import java.net.URI;
import java.util.Map.Entry;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author phoenix
 */
public class ClusterI extends Configured implements Tool {

    int level;
    double threshold;

    public ClusterI() {
        this.threshold = AppConfig.THRESHOLD;
    }

    public ClusterI(int level) {
        this.level = level;
        this.threshold = AppConfig.THRESHOLD + level;
    }

    @Override
    public int run(String[] strings) throws Exception {
        String fileNameInput = "/cluster/matrix_result_"+level+".seq/part-r-00000";
        String fileNameOutput = "/cluster/lookup_"+level+".seq";

        Configuration config = new Configuration();
        FileSystem fsInput = FileSystem.get(URI.create(fileNameInput), config);
        FileSystem fsOutput = FileSystem.get(URI.create(fileNameOutput), config);

        Path pathInput = new Path(fileNameInput);
        Path pathOutput = new Path(fileNameOutput);

        List<String> keyList = new ArrayList<String>();
        List<String> mergeList = new ArrayList<String>();
        Set<String> mergeSet = new HashSet<String>();
        Map<String, Boolean> passMap = new TreeMap<String,Boolean>();

        SequenceFile.Reader reader = null;
        SequenceFile.Writer writer = null;

        Text key = new Text();
        StringBuilder keyBuilder;

        try {
            reader = new SequenceFile.Reader(fsInput, pathInput, config);
            writer = SequenceFile.createWriter(fsOutput, config, pathOutput, Text.class, Text.class);

            Writable k = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
            Writable v = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), config);

            //format of v is img1.jpg,img2.jpg, or img1.jpg,
            System.out.println("Clustering New I1:");
            while (reader.next(k, v)) {
                long curPosition = reader.getPosition();

                String readKey = k.toString();
                //System.out.println("key:" + readKey);

                String readValue = v.toString();
                readValue = readValue.substring(0, readValue.length() - 1); //remove last comma
                String[] elements = readValue.split(":");

                keyBuilder = new StringBuilder();

                double distortion = Double.parseDouble(readKey);
                if (distortion < threshold) {

                    //check if already merged, skip if already merged
                    if (mergeSet.contains(elements[0])) {
                        if (!mergeSet.contains(elements[1])) {
                            passMap.put(elements[1], Boolean.FALSE);
                        }
                        continue;
                    }
                    if (mergeSet.contains(elements[1])) {
                        if (!mergeSet.contains(elements[0])) {
                            passMap.put(elements[0], Boolean.FALSE);
                        }
                        continue;
                    }

                    //if not merged then merge them
                    mergeSet.add(elements[0]);
                    mergeSet.add(elements[1]);
                    passMap.put(elements[0], Boolean.TRUE);
                    passMap.put(elements[1], Boolean.TRUE);
                    keyBuilder.append(elements[0] + "," + elements[1]);


                    while (reader.next(k, v)) {
                        double dist = Double.parseDouble(k.toString());
                        if (dist - distortion > AppConfig.THRESHOLD_DEVIATION) {
                            break;
                        }
//                        System.out.println("dist is ok");
                        String newRecordValue = v.toString();
                        newRecordValue = newRecordValue.substring(0, newRecordValue.length() - 1);
                        String[] newElements = newRecordValue.split(":");

                        //init passmap: to keep record of not merged elements
                        if (!passMap.containsKey(newElements[0])) {
                            passMap.put(newElements[0], Boolean.FALSE);
                        }
                        if (!passMap.containsKey(newElements[1])) {
                            passMap.put(newElements[1], Boolean.FALSE);
                        }

                        //check if newElements are already merged; skip if merged
                        if (mergeSet.contains(newElements[0]) || mergeSet.contains(newElements[1])) {
                            //System.out.println("mergeSet.contains(newElements[0]):"+mergeSet.contains(newElements[0]));
                            //System.out.println("mergeSet.contains(newElements[1]):"+mergeSet.contains(newElements[1]));
                            
                        }else{
//                            System.out.println("inside intersection\n");
//                            System.out.print("elements are:");
//                            System.out.print(elements[0]+",");
//                            System.out.print(elements[1]+",");
//                            System.out.print(newElements[0]+",");
//                            System.out.print(newElements[1]+",");
                            //check if there are common elements
                            if (intersect(elements, newElements)) {
//                                System.out.println("inside intersection\n");
                                mergeSet.add(newElements[0]);
                                mergeSet.add(newElements[1]);
                                passMap.put(newElements[0], Boolean.TRUE);
                                passMap.put(newElements[1], Boolean.TRUE);
                                keyBuilder.append("," + newElements[0] + "," + newElements[1]);
                            }
                        }
                    }

                    reader.seek(curPosition);
                    writer.append(new Text(keyBuilder.toString()), new Text("#"));
                    System.out.println("output key:" + keyBuilder.toString());
                } else {
                    if (!passMap.containsKey(elements[0])) {
                        passMap.put(elements[0], Boolean.FALSE);
                    }
                    if (!passMap.containsKey(elements[1])) {
                        passMap.put(elements[1], Boolean.FALSE);
                    }
                }
//                System.out.println(" output key:"+keyBuilder.toString());
            }
            for(Entry e:passMap.entrySet()){
                if(!(Boolean)e.getValue()){
                    writer.append(new Text(e.getKey().toString()), new Text("#"));
                    System.out.println("output key:"+e.getKey());
                }
                
            }
            System.out.println("\nclustering: complete");
        } finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(writer);
        }
        return 1;
    }

    public boolean checkPairAlreadyMerged(String[] key, Map passMap, Set mergeSet) {
        for (String k : key) {
            if (passMap.get(k) == null) {
                passMap.put(k, Boolean.FALSE);
            }
        }
        if (mergeSet.contains(key[0]) && !mergeSet.contains(key[1]) && passMap.get(key[1]) == Boolean.FALSE) {
            passMap.put(key[1], Boolean.TRUE);
            return true;
        }
        if (mergeSet.contains(key[1]) && !mergeSet.contains(key[0]) && passMap.get(key[0]) == Boolean.FALSE) {
            passMap.put(key[0], Boolean.TRUE);
            return true;
        }
        return false;
    }

    public boolean intersect(String[] pair1, String[] pair2) {
        for (String k1 : pair1) {
            for (String k2 : pair2) {
                if (k1.equals(k2)) {
                    return true;
                }
            }
        }
        return false;
    }
}