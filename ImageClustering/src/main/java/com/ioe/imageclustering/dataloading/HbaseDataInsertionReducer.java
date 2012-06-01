/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ioe.imageclustering.dataloading;

import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author phoenix
 */
public class HbaseDataInsertionReducer extends Reducer<BooleanWritable, Text, NullWritable, NullWritable> {

        @Override
        public void reduce(BooleanWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //context.write(new BooleanWritable(true), new Text("reduce successful"));
        }
    }
