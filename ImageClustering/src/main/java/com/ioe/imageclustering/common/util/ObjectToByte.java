/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ioe.imageclustering.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 *
 * @author phoenix
 */
public class ObjectToByte {
    
    // Convert an object to a byte array
    public static byte[] objectToByteArray(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        byte[] outputByteArray = bos.toByteArray();
        out.close();
        bos.close();
        return outputByteArray;
    }
    
    // Convert a byte array to an Object
    public static Object ByteArrayToObject(byte[] dataBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(dataBytes);
        ObjectInput in = new ObjectInputStream(bis);
        Object o = in.readObject();

        bis.close();
        in.close();
        return o;
    }
}
