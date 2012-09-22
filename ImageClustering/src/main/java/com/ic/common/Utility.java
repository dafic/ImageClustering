/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author phoenix
 */
public class Utility {

    public static byte[] readBytes(InputStream stream) throws IOException {
        if (stream == null) {
            return new byte[]{};
        }
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        boolean error = false;
        try {
            int numRead = 0;
            while ((numRead = stream.read(buffer)) > -1) {
                output.write(buffer, 0, numRead);
            }
        } catch (IOException e) {
            error = true; // this error should be thrown, even if there is an error closing stream
            throw e;
        } catch (RuntimeException e) {
            error = true; // this error should be thrown, even if there is an error closing stream
            throw e;
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                if (!error) {
                    throw e;
                }
            }
        }
        output.flush();
        return output.toByteArray();
    }
    public static String getNameFromURI(String uri){
        String str=uri.substring(uri.lastIndexOf("/")+1);
        String extension=str.substring(str.indexOf("."));
        String md5=DigestUtils.md5Hex(str);
        return md5+extension;
    }
    public static String getNameFromGmmURI(String uri){
        String gmmOnly=uri.substring(uri.lastIndexOf("/")+1);
        String gmmNameOnly=gmmOnly.substring(0,gmmOnly.indexOf("_"));
        String extension=gmmNameOnly.substring(gmmNameOnly.indexOf("."));
        String md5=DigestUtils.md5Hex(gmmNameOnly);
        return md5+extension;
    }
}
