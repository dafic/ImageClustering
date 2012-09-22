/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.common;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import javax.imageio.ImageIO;

/**
 *
 * @author phoenix
 */
public class ByteImageConversion {

    public static byte[] imageToByte(String filename, String fileType) throws IOException {
        byte[] imageInByte = null;
        BufferedImage originalImage = ImageIO.read(new File(filename));
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            ImageIO.write(originalImage, fileType, baos);
            baos.flush();
            imageInByte = baos.toByteArray();
            baos.close();

        } catch (Exception e) {
            System.out.println("error converting image to bytes \n");
        }
        return imageInByte;
    }

    public static BufferedImage byteToBufferedImage(byte[] imageInByte) throws IOException {
        return ImageIO.read(new ByteArrayInputStream(imageInByte));
    }
}
