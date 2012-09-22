package com.ic.cluster.sequential;


import com.ic.admin.HDFSClient;
import jMEF.MixtureModel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author bhanu
 */
public class SeqClusterMain {

    /**
     * @param args the command line arguments
     */
     public static void main(String[] args) throws IOException {
        String fileName = "data.gmm";
         HDFSClient client=new HDFSClient();
         client.deleteFile(fileName);
         //client.mkdir("/sequential/gmm.seq/");
        //String fileName = "/cluster/testgmm.seq/part-m-00000";
        //String fileName = "/sequential/gmm.seq/part-m-00000";
        List<MixtureModel> mList = new ArrayList<MixtureModel>();
        List<String> newGMM = new ArrayList<String>();
        List<String> kList = new ArrayList<String>();
        Map<Double, String> map = new TreeMap<Double,String>();
        Configuration config = new Configuration();

        SeqCluster sg = new SeqCluster(fileName, config);
        sg.createFile();
        sg.read();
        
        sg.createMatrix(map, kList, mList);
        sg.cluster(map, kList, mList);


        boolean flag = true;
        String fileName2 = "result1.gmm";
        while (flag) {

            mList = new ArrayList<MixtureModel>();
            newGMM = new ArrayList<String>();
            kList = new ArrayList<String>();
            map = new TreeMap<Double,String>();
            SeqCluster sg2 = new SeqCluster(fileName2, config);
            sg2.createMatrix2(map, kList, mList);
            flag = sg2.cluster2(map, kList, mList);
            sg2.read2();
            fileName2 = "result2.gmm";
            if (!flag) {
                sg2.writeToHbase();
            }
        }
       /* */
    }
}
