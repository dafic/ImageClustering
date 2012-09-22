/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.admin;

/**
 *
 * @author phoenix
 */
public class AppConfig {
    /** application configuration related to Hadoop*/
    public static final String HADOOP_HOME="/usr/local/hadoop";
    public static final String HBASE_MASTER="127.0.0.1:60000";
    
    public static final String LOCALHOST="http://localhost";
    public static final String IMAGE_LOCATION="/opt/lampp/htdocs/image_and_gmm/dataset/";
    public static final String GMM_LOCATION="/opt/lampp/htdocs/image_and_gmm/gmm/";
    
    public static final double THRESHOLD=2;
    public static final double THRESHOLD_DEVIATION=0.5;
    
    public static final String CLUSTER_DATABASE="ClusterDatabase";
    public static final String CLUSTER_DATABASE_CF="Cluster";//column family of ClusterDatabase
    public static final String IMAGE_DATABASE="ImageDatabase";
    public static final String IMAGE_DATABASE_CF="Image";//column family of ImageDatabase
}
