/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.stat;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.jfree.data.xy.XYSeries;

public class Statistics {
    
    public static double  slopeByLeastSquareBestLineFit(double[] x, double[] y) throws Exception {
        if (x == null || y == null) {
            throw new Exception("The arrays supplied are null");
        }
        if (x.length == y.length) {
            double sumxy = 0;
            double sumx = 0;
            double sumy = 0;
            double sumx_sqr = 0;
            int n = x.length;
            for (int i = 0; i < n; i++) {
                sumxy +=  x[i]*y[i];
                sumx += x[i];
                sumy += y[i];
                sumx_sqr += x[i]*x[i];
            }
            return (n*sumxy - sumx*sumy)/(n*sumx_sqr - sumx*sumx);
        }else 
            throw new Exception("Array lengths of inputs are not equal");
    }
    

    public static void setLeastSquareBestLine(double x[], double y[], XYSeries series) throws Exception {
        double slope = slopeByLeastSquareBestLineFit(x,y);
        double intercept = interceptByLeastSquareBestLineFit(x,y);
        int n = x.length;
        for (int i = 0; i < n; i++) {
            series.add(x[i],slope*x[i]+intercept);
        }
    }

    public static double interceptByLeastSquareBestLineFit(double x[], double y[]) throws Exception {
        if (x == null || y == null) {
            throw new Exception("The arrays supplied are null");
        }
        if (x.length == y.length) {
            double sumxy = 0;
            double sumx = 0;
            double sumy = 0;
            double sumx_sqr = 0;
            int n = x.length;
            for (int i = 0; i < n; i++) {
                sumxy +=  x[i]*y[i];
                sumx += x[i];
                sumy += y[i];
                sumx_sqr += x[i]*x[i];
            }
            return (sumy*sumx_sqr-sumx*sumxy)/(n*sumx_sqr - sumx*sumx);
        }else 
            throw new Exception("Array lengths of inputs are not equal");
    }
    
    public static double getAverage(double[] x) throws Exception{
        if (x == null) throw new Exception("getAverage Input supplied is null");
        if (x.length == 0) throw new Exception("getAverage No data in the input");
        double sum = 0;
        for (int i =0; i < x.length; i++) {
            sum += x[i];
        }
        return sum/x.length;
    }
    
    public static double linearCorrelation(double[] x, double[] y) throws Exception {
        if (x == null || y == null) {
            throw new Exception("The arrays supplied are null");
        }
        if (x.length == y.length) {
            double sumxy = 0;
            double sumx = 0;
            double sumy = 0;
            double sumx_sqr = 0;
            double sumy_sqr = 0;
            int n = x.length;
            for (int i = 0; i < n; i++) {
                sumxy +=  x[i]*y[i];
                sumx += x[i];
                sumy += y[i];
                sumx_sqr += x[i]*x[i];
                sumy_sqr += y[i]*y[i];
            }
            double scxy = sumxy - (sumx*sumy)/n;
            double sx = sumx_sqr - (sumx*sumx)/n;
            double sy = sumy_sqr - (sumy*sumy)/n;
            return scxy/(Math.sqrt(sx*sy));
        }else 
            throw new Exception("Array lengths of inputs are not equal");
    }
    
    
    private double sum(double[] x) {
        double sum = 0;
        if (x == null) return sum;
        for (int i = 0; i < x.length; i++) sum += x[i];
        return sum;
    }
}
