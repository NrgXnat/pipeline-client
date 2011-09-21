/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;

import org.apache.xmlbeans.XmlOptions;
import org.nrg.pipeline.xmlbeans.ParameterData;
import org.nrg.pipeline.xmlbeans.ParametersDocument;
import org.nrg.pipeline.xmlbeans.ParameterData.Values;
import org.nrg.pipeline.xmlbeans.ParametersDocument.Parameters;

import com.Ostermiller.util.CSVParser;

public class ParameterToFile {
	
	  int noOfRequiredArguments = 2;
	  int noOfRequiredArgumentsAvailable = 0;
	  String parameterFile = null;
	  Parameters params;
	  
	public ParameterToFile( String argv[]) {
        int c;
        params = ParametersDocument.Parameters.Factory.newInstance();
        LongOpt[] longopts = new LongOpt[3];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[1] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f'); 
        longopts[2] = new LongOpt("parameter", LongOpt.REQUIRED_ARGUMENT, null, 'p');

        Getopt g = new Getopt("ParameterToFile", argv, "f:p:h;", longopts, true);
        g.setOpterr(false); // We'll do our own error handling
        //
        while ((c = g.getopt()) != -1) {
          switch (c)
            {
               case 'f':
            	   parameterFile = g.getOptarg();
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'p':
                   addParameter(g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
            }
        }
        
        if (noOfRequiredArgumentsAvailable < noOfRequiredArguments) {
            System.out.println("Missing required arguments");
            printUsage();
        }
	}
	
	 public void printUsage() {
	        String usage = "ParameterToFile  \n";
	        usage += "Options:\n";
	        usage += "\t -parameter: FORMAT: <param name>=<comma separated values> \n";
	        usage += "\t -file: The file path which will be a Params documents\n";
	        usage += "\t -help\n";
	        System.out.println(usage);
	        System.exit(1);
	    }
	
	 public void save() throws Exception{
		 File outFile = new File(parameterFile);
		 ParametersDocument paramDoc = ParametersDocument.Factory.newInstance();
	     paramDoc.addNewParameters().set(params);
		 paramDoc.save(outFile, new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces());
	 }
	
	  private void addParameter(String paramValuePair) {
	        paramValuePair = paramValuePair.trim();
	        String parts[] = paramValuePair.split("=");
	        if (parts == null || parts.length < 2  ) {
	            System.out.println("Invalid parameter found: " + paramValuePair);
	            printUsage();
	            System.exit(1);
	        }
	        String paramName = parts[0].trim();
	        String paramValues = parts[1].trim();
	        ParameterData pData = params.addNewParameter();
	        pData.setName(paramName);
	        Values values = pData.getValues();
	        if (values == null) values = pData.addNewValues();
	        String[][] str = CSVParser.parse(paramValues);
	        if (str==null || str.length != 1) {
	            System.out.println("Invalid parameter found: " + paramValuePair);
	            System.out.println("NOTE: If a parameter includes a comma or a new line, the whole field must be surrounded with double quotes.");
	            System.out.println("When the field is in quotes, any quote literals must be escaped by \" Backslash literals must be escaped by \\"); 
	            System.out.println("Otherwise a backslash and the character following will be treated as the following character, IE. \"\n\" is equivalent to \"n\".");
	            System.out.println("Text that comes after quotes that have been closed but come before the next comma will be ignored.");
	            printUsage();
	            System.exit(1);
	        }
	        if (str[0].length == 1) {
	            values.setUnique(str[0][0]);
	        }else {
	            for (int i = 0; i < str[0].length; i++) {
	                values.addList(str[0][i]);
	            }
	        }
	  }
	
	public static void main(String args[]) {
		try {
			ParameterToFile paramToFile = new ParameterToFile(args);
			paramToFile.save();
			System.exit(0);
		}catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
