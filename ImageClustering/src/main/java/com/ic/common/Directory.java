package com.ic.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * contains the common method for creating the list of url
 * @author phoenix
 */
public class Directory {
    /**
     * creates the list of url with given prefix to all file present in given directory in file
     * @param sourceDirectory contains the files
     * @param prefix of the url e.g. http://localhost/dataset/
     * @param outputFilename : name for the output list file.
     */
    public static void createList(String sourceDirectory,String prefix,String outputFilename) {
        File folder = new File(sourceDirectory);
        StringBuilder list=new StringBuilder("");

        File[] listOfFiles = folder.listFiles();
        if (listOfFiles.length == 0) {
            System.out.println("There are no files");
        }
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                list.append(prefix+listOfFiles[i].getName());
                list.append("\n");
            } else if (listOfFiles[i].isDirectory()) {
                System.out.println("Directory " + listOfFiles[i].getName());
            }
            if(i%10==0){
                writeFile(outputFilename,list.toString());
                list=new StringBuilder();
            }
        }
        writeFile(outputFilename,list.toString());
    }

    /**
     * writes(append) to file the given string
     * @param fileName :the output file
     * @param value :string to write
     */
    public static void writeFile(String fileName,String value) {
        try {          
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
            writer.append(value);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
