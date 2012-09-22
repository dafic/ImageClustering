/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ic.common;

/**
 *
 * @author phoenix
 */
import java.awt.*;
import java.awt.image.*;
import java.io.*;
import javax.imageio.ImageIO;

public class RGBtoLAB {

    public static void main(String[] args) {

        //get input image
        String fileName = "rgb1.jpg";
        //read input image
        BufferedImage image = null;
        try {
            image = ImageIO.read(new File(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }


        //setup result image
        int sizeX = image.getWidth();
        int sizeY = image.getHeight();


        float r, g, b, X, Y, Z, fx, fy, fz, xr, yr, zr;
        float ls, as, bs;
        float eps = 216.f / 24389.f;
        float k = 24389.f / 27.f;

        float Xr = 0.964221f;  // reference white D50
        float Yr = 1.0f;
        float Zr = 0.825211f;

        for (int y = 0; y < image.getHeight(); y++) {
            for (int x = 0; x < image.getWidth(); x++) {
                int c = image.getRGB(x, y);
                int R = (c & 0x00ff0000) >> 16;
                int G = (c & 0x0000ff00) >> 8;
                int B = c & 0x000000ff;

                r = R / 255.f; //R 0..1
                g = G / 255.f; //G 0..1
                b = B / 255.f; //B 0..1

                // assuming sRGB (D65)
                if (r <= 0.04045) {
                    r = r / 12;
                } else {
                    r = (float) Math.pow((r + 0.055) / 1.055, 2.4);
                }

                if (g <= 0.04045) {
                    g = g / 12;
                } else {
                    g = (float) Math.pow((g + 0.055) / 1.055, 2.4);
                }

                if (b <= 0.04045) {
                    b = b / 12;
                } else {
                    b = (float) Math.pow((b + 0.055) / 1.055, 2.4);
                }


                X = 0.436052025f * r + 0.385081593f * g + 0.143087414f * b;
                Y = 0.222491598f * r + 0.71688606f * g + 0.060621486f * b;
                Z = 0.013929122f * r + 0.097097002f * g + 0.71418547f * b;

                // XYZ to Lab
                xr = X / Xr;
                yr = Y / Yr;
                zr = Z / Zr;

                if (xr > eps) {
                    fx = (float) Math.pow(xr, 1 / 3.);
                } else {
                    fx = (float) ((k * xr + 16.) / 116.);
                }

                if (yr > eps) {
                    fy = (float) Math.pow(yr, 1 / 3.);
                } else {
                    fy = (float) ((k * yr + 16.) / 116.);
                }

                if (zr > eps) {
                    fz = (float) Math.pow(zr, 1 / 3.);
                } else {
                    fz = (float) ((k * zr + 16.) / 116);
                }

                ls = (116 * fy) - 16;
                as = 500 * (fx - fy);
                bs = 200 * (fy - fz);


                int Ls = (int) (2.55 * ls + .5);
                int As = (int) (as + .5);
                int Bs = (int) (bs + .5);

                int lab = 0xFF000000 + (Ls << 16) + (As << 8) + Bs; // and reassign

                image.setRGB(x, y, lab);

            }
        }


        //write new image
        File outputfile = new File("lab1.png");
        try {
            // png is an image format (like gif or jpg)
            ImageIO.write(image, "png", outputfile);
        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }
}
