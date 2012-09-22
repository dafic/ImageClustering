package com.ic.admin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HDFSClient {
    Configuration config;
    /**
     * class constructor
     * @param AppConfig.HADOOP_HOME: home directory of HADOOP
     */
    public HDFSClient() {
        this.config=new Configuration();
        config.addResource(new Path(AppConfig.HADOOP_HOME+"/conf/core-site.xml"));
        config.addResource(new Path(AppConfig.HADOOP_HOME+"/conf/hdfs-site.xml"));
        config.addResource(new Path(AppConfig.HADOOP_HOME+"/conf/mapred-site.xml"));
    }
    /**
     * class constructor
     * @param config : Configuration parameters
     */
    public HDFSClient(Configuration config){
        this.config=config;
    }
    /**
     * sets the configuration of the HDFS client
     * @param config 
     */
    public void setConfiguration(Configuration config){
        this.config=config;
    }
    /**
     * return configuration of the HDFS client
     * @return configuration of the HDFS client
     */
    public Configuration getConfig() {
        return config;
    }
    
    /**
     * prints the usage of different methods available on HDFSClient object
     */
    public static void printUsage() {
        System.out.println("Usage: hdfsclient add" + "<local_path> <hdfs_path>");
        System.out.println("Usage: hdfsclient read" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient delete" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient mkdir" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient copyfromlocal" + "<local_path> <hdfs_path>");
        System.out.println("Usage: hdfsclient copytolocal" + " <hdfs_path> <local_path> ");
        System.out.println("Usage: hdfsclient modificationtime" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient getblocklocations" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient gethostnames");
    }

    /**
     * checks if the given path exists
     * @param source path
     * @return true if the path exists
     * @throws IOException 
     */
    public boolean ifExists(Path source) throws IOException {
        FileSystem hdfs = FileSystem.get(config);
        boolean isExists = hdfs.exists(source);
        return isExists;
    }
    /**
     * prints the Host Names of the data nodes
     * @throws IOException 
     */
    public void getHostnames() throws IOException {
        FileSystem fs = FileSystem.get(config);
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        String[] names = new String[dataNodeStats.length];
        for (int i = 0; i < dataNodeStats.length; i++) {
            names[i] = dataNodeStats[i].getHostName();
            System.out.println((dataNodeStats[i].getHostName()));
        }
    }

    /**
     * prints the block locations of input source path file
     * @param source
     * @throws IOException 
     */
    public void getBlockLocations(String source) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path(AppConfig.HADOOP_HOME+"/conf/core-site.xml"));
        conf.addResource(new Path(AppConfig.HADOOP_HOME+"/conf/hdfs-site.xml"));
        conf.addResource(new Path(AppConfig.HADOOP_HOME+"/conf/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.get(conf);
        Path srcPath = new Path(source);

        // Check if the file already exists
        if (!(ifExists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fileSystem.getFileStatus(srcPath);

        BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        int blkCount = blkLocations.length;

        System.out.println("File :" + filename + "stored at:");
        for (int i = 0; i < blkCount; i++) {
            String[] hosts = blkLocations[i].getHosts();
            System.out.format("Host %d: %s %n", i, hosts);
        }

    }

    /**
     * prints the modification time of the file
     * @param source filename
     * @throws IOException 
     */
    public void getModificationTime(String source) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        // Check if the file already exists
        if (!(fileSystem.exists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
        long modificationTime = fileStatus.getModificationTime();

        System.out.format("File %s; Modification time : %0.2f %n", filename, modificationTime);

    }

    /**
     * copy file from local source to HDFS destination
     * @param source path
     * @param dest path
     * @throws IOException 
     */
    public void copyFromLocal(String source, String dest) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(dstPath))) {
            System.out.println("No such destination " + dstPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try {
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            System.out.println("File " + filename + "copied to " + dest);
        } catch (Exception e) {
            System.err.println("Exception caught! :" + e);
            System.exit(1);
        } finally {
            fileSystem.close();
        }
    }

    /**
     * copy the file from HDFS to local filesystem
     * @param source path in HDFS
     * @param dest path in local filesystem
     * @throws IOException 
     */
    public void copyToLocal(String source, String dest) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try {
            fileSystem.copyToLocalFile(srcPath, dstPath);
            System.out.println("File " + filename + "copied to " + dest);
        } catch (Exception e) {
            System.err.println("Exception caught! :" + e);
            System.exit(1);
        } finally {
            fileSystem.close();
        }
    }

    /**
     * rename the file
     * @param fromthis: original name
     * @param tothis: new name 
     * @throws IOException 
     */
    public void renameFile(String fromthis, String tothis) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        Path fromPath = new Path(fromthis);
        Path toPath = new Path(tothis);

        if (!(fileSystem.exists(fromPath))) {
            System.out.println("No such destination " + fromPath);
            return;
        }

        if (fileSystem.exists(toPath)) {
            System.out.println("Already exists! " + toPath);
            return;
        }

        try {
            boolean isRenamed = fileSystem.rename(fromPath, toPath);
            if (isRenamed) {
                System.out.println("Renamed from " + fromthis + "to " + tothis);
            }
        } catch (Exception e) {
            System.out.println("Exception :" + e);
            System.exit(1);
        } finally {
            fileSystem.close();
        }

    }

    /**
     * add a file to a destination folder
     * @param source file 
     * @param dest folder
     * @throws IOException 
     */
    public void addFile(String source, String dest) throws IOException {

        FileSystem fileSystem = FileSystem.get(config);

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        // Create the destination path including the filename.
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + "/" + filename;
        } else {
            dest = dest + filename;
        }

        // Check if the file already exists
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        // Create a new file and write data to it.
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File(source)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        // Close all the file descripters
        in.close();
        out.close();
        fileSystem.close();
    }

    /**
     * read a file 
     * @param file in HDFS
     * @throws IOException 
     */
    public void readFile(String file) throws IOException {
       
        FileSystem fileSystem = FileSystem.get(config);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);

        String filename = file.substring(file.lastIndexOf('/') + 1,
                file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(
                new File(filename)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    /**
     * delete file in HDFS
     * @param file path in HDFS
     * @throws IOException 
     */
    public void deleteFile(String file) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    /**
     * make a directory in HDFS
     * @param dir name
     * @throws IOException 
     */
    public void mkdir(String dir) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already exists!");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }

}
