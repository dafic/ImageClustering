/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.cluster;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author phoenix
 */
public class SequenceFileReaderDemo {

    public static void main(String[] args) throws IOException {
//        String uri = "/cluster/lookup1.seq";
//        String uri = "/cluster/matrix_intermediate.seq/part-r-00000";
        String uri = "/cluster/matrix_intermediate_1.seq/part-r-00000";
//         String uri = "/cluster/matrix_final_v4.seq/part-r-00000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        
        
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            
            System.out.println("starting reading\n\n");
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
//                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                System.out.printf("%s : %s\n", key, value);
                position = reader.getPosition(); // beginning of next record
            }
            System.out.println("completed reading\n\n");
            
        } finally {
            IOUtils.closeStream(reader);
        }
        
    }
}