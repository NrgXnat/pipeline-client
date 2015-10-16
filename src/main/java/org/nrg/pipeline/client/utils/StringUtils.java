/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.utils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class StringUtils {
    
    private static DecimalFormat df =
           new DecimalFormat("0.00", new DecimalFormatSymbols(Locale.US));
    
    private static int dfDigits = 2;

    /** Converts a number to a rounded formatted string.
       The 'precision' argument specifies the number of
       digits to the right of the decimal point. */
    public static String d2s(double n, int precision) {
       if (n==Float.MAX_VALUE) // divide by 0 in FloatProcessor
           return "3.4e38";
       boolean negative = n<0.0;
       if (negative)
           n = -n;
       double whole = Math.round(n * Math.pow(10, precision));
       double rounded = whole/Math.pow(10, precision);
       if (negative)
           rounded = -rounded;
       if (precision!=dfDigits)
           switch (precision) {
               case 0: df.applyPattern("0"); dfDigits=0; break;
               case 1: df.applyPattern("0.0"); dfDigits=1; break;
               case 2: df.applyPattern("0.00"); dfDigits=2; break;
               case 3: df.applyPattern("0.000"); dfDigits=3; break;
               case 4: df.applyPattern("0.0000"); dfDigits=4; break;
               case 5: df.applyPattern("0.00000"); dfDigits=5; break;
               case 6: df.applyPattern("0.000000"); dfDigits=6; break;
               case 7: df.applyPattern("0.0000000"); dfDigits=7; break;
               case 8: df.applyPattern("0.00000000"); dfDigits=8; break;
           }
       String s = df.format(rounded);
       return s;
    }
    
    private String escapeSpecialShellCharacters(String input) {

        String rtn=input;
        
        rtn= org.apache.commons.lang.StringUtils.replace(rtn,"\"","\\\"");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"{", "\\{");

        rtn= org.apache.commons.lang.StringUtils.replace(rtn,"}","\\}");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"'","\\'");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"`","\\`");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"*","\\*");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"?","\\?");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"[","\\[");
        
        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"]","\\]");
        
        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"\\","\\\\");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"~","\\~");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"$","\\$");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"!","\\!");

        rtn= org.apache.commons.lang.StringUtils.replace(rtn,"&","\\&");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,";","\\;");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"(","\\(");

        rtn= org.apache.commons.lang.StringUtils.replace(rtn,")","\\)");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"<","\\<");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,">","\\>");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"|","\\|");

        rtn=org.apache.commons.lang.StringUtils.replace(rtn,"#","\\#");

        rtn= org.apache.commons.lang.StringUtils.replace(rtn,"@","\\@");

        return rtn;

       }
}
