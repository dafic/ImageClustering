/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.common;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;

/**
 *
 * @author phoenix
 */
public class ImageUtil {
    private static final int IMG_WIDTH = 500;
    private static final int IMG_HEIGHT = 100;
    
    public static BufferedImage resizeImage(BufferedImage originalImage, int type){
	BufferedImage resizedImage = new BufferedImage(IMG_WIDTH, (int)(IMG_WIDTH*originalImage.getHeight()/originalImage.getWidth()), type);
	Graphics2D g = resizedImage.createGraphics();
	g.drawImage(originalImage, 0, 0, IMG_WIDTH, (int)(IMG_WIDTH*originalImage.getHeight()/originalImage.getWidth()), null);
	g.dispose();
 
	return resizedImage;
    }
}
