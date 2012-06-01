/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ioe.imageclustering.gmm.utilites;

import com.ioe.imageclustering.gmm.jmef.MixtureModel;
import java.awt.image.BufferedImage;

/**
 *
 * @author phoenix
 */
public class GMMtest {
    /**
	 * Main function.
	 * @param args
	 */
	public static void main(String[] args) {
		
		// Display
		String title = "";
		title += "+----------------------------------------+\n";
		title += "| Statistical images from mixture models |\n";
		title += "+----------------------------------------+\n";
		System.out.print(title);

		// Variables
		int n = 8;
		
		// Image/texture information
		String input_folder  = "sample/data/";
		String output_folder = "sample/gmm/";
		String image_name    = "img1";
		String image_path    = input_folder + image_name + ".jpg";
		String mixture_path  = String.format("%s%s_5D_%03d.mix", output_folder, image_name, n); 
		//String mixture_path  = "bear_032.mix"; 
		
		// Read image and generate initial mixture model
		System.out.print("Read image and generate/load the mixture 5D : ");
		BufferedImage image = Image.readImage(image_path);
		MixtureModel  f     = Image.loadMixtureModel(mixture_path, image, n);
		System.out.println("ok");
		
		// Creates and save the statistical image
		System.out.print("Create the statistical image                : ");
		BufferedImage stat = Image.createImageFromMixtureModel(image.getWidth(), image.getHeight(), f);
		Image.writeImage(stat, String.format("%sS_%s_dist_%03d.png", output_folder, image_name, n));
		System.out.println("ok");

		// Creates and save the ellipse image
		System.out.print("Create the ellipse image                    : ");
		BufferedImage ell = Image.createEllipseImage(image.getWidth(), image.getHeight(), f, 2);
		Image.writeImage(ell, String.format("%sS_%s_ell_%03d.png", output_folder, image_name, n));
		System.out.println("ok");
		
	}
}
