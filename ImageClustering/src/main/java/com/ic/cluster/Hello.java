/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.cluster;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author phoenix
 */
public class Hello {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        FileReader reader=new FileReader("newfile");
        BufferedReader r=new BufferedReader(reader);
        Set<String> set=new HashSet<String>();
        String line=null;
        while((line=r.readLine())!=null){
            set.add(line);
        }
        System.out.println("set size:"+set.size());
    }
}
