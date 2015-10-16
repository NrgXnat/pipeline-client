/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class BatchCommandLineArguments extends AbsVersion{

    /**
     * @return Returns the stopAtException.
     */
    public boolean isStopAtException() {
        return stopAtException;
    }

    public BatchCommandLineArguments(String argv[]) {
        int c;
        commandLineArgs = new Hashtable();
        fixedArguments = new Vector();
        LongOpt[] longopts = new LongOpt[15];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[1] = new LongOpt("pipeline", LongOpt.REQUIRED_ARGUMENT, null, 'p'); 
        longopts[2] = new LongOpt("csv", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[3] = new LongOpt("startAt", LongOpt.REQUIRED_ARGUMENT, null, 's');
        longopts[4] = new LongOpt("notify", LongOpt.REQUIRED_ARGUMENT, null, 'e');
        longopts[5] = new LongOpt("dataType", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        longopts[6] = new LongOpt("debug", LongOpt.NO_ARGUMENT, null, 'g');
        longopts[7] = new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'u');
        longopts[8] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'w');
        longopts[9] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[10] = new LongOpt("noqueue", LongOpt.NO_ARGUMENT, null, 'q');
        longopts[11] = new LongOpt("catalogPath", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        longopts[12] = new LongOpt("continueOnException", LongOpt.NO_ARGUMENT, null, 't');
        longopts[13] = new LongOpt("log", LongOpt.REQUIRED_ARGUMENT, null, 'z');
        longopts[14] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'v');
        
        // 
        Getopt g = new Getopt("XNATPipelineBatchLauncher", argv, "p:c:s:e:d:u:w:o:l:z:ghqtv;", longopts, true);
        g.setOpterr(false); // We'll do our own error handling
        //
        while ((c = g.getopt()) != -1) {
          switch (c)
            {
                case 'l':
                    commandLineArgs.put("catalogPath",g.getOptarg());
                    fixedArguments.add("-catalogPath");
                    fixedArguments.add(g.getOptarg());
                    break;
                case 'p':
                   commandLineArgs.put("pipeline",g.getOptarg());
                   fixedArguments.add("-pipeline");
                   fixedArguments.add(g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'c':
                   commandLineArgs.put("csv",g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 's':
                   commandLineArgs.put("startAt",g.getOptarg());
                   fixedArguments.add("-startAt");
                   fixedArguments.add(g.getOptarg());
                   break;
               case 'e':
                   addEmail(g.getOptarg());
                   fixedArguments.add("-notify");
                   fixedArguments.add(g.getOptarg());
                   break;
               case 'd':
                   commandLineArgs.put("dataType",g.getOptarg());
                   fixedArguments.add("-dataType");
                   fixedArguments.add(g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'u':
                   commandLineArgs.put("username",g.getOptarg());
                   fixedArguments.add("-u");
                   fixedArguments.add(g.getOptarg());
                   fixedArguments.add("-parameter");
                   fixedArguments.add("u=" + g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'w':
                   commandLineArgs.put("password",g.getOptarg());
                   fixedArguments.add("-pwd");
                   fixedArguments.add(g.getOptarg());
                   fixedArguments.add("-parameter");
                   fixedArguments.add("pwd="+g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'o':
                   String host = g.getOptarg();
                   if (!host.endsWith("/")) host+="/";
                   commandLineArgs.put("host",host);
                   fixedArguments.add("-host");
                   fixedArguments.add(g.getOptarg());
                   fixedArguments.add("-parameter");
                   fixedArguments.add("host="+g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'g':
                   commandLineArgs.put("debug",new Boolean(true));
                   break;
               case 'h':
                 printUsage();
                 break;
               case 'q':
                   noqueue = true;
                   break;
               case 't':
                   stopAtException = false;
                   break;
               case 'v':
                   echoVersion();
                   System.exit(0);                   
               case 'z':
                   commandLineArgs.put("log",g.getOptarg());
                   break;
               default:
                 System.out.println("Invalid argument recieved " + c);  
                 printUsage();
                 break;
            }
        }
        if (noOfRequiredArgumentsAvailable != noOfRequiredArguments) {
            System.out.println("Missing required arguments");
            printUsage();
        }
    }
    
    private void addEmail(String emailId) {
        if (commandLineArgs.containsKey("notify")) {
            ((ArrayList)commandLineArgs.get("notify")).add(emailId);
        }else {
            ArrayList emails = new ArrayList(); emails.add(emailId);
            commandLineArgs.put("notify",emails);
        }
    }
    
    public void printUsage() {
        String usage = "XNATPipelineBatchLauncher  \n";
        usage += "Options:\n";
        usage += "\t -pipeline <path to pipeline xml file>\n";
        usage += "\t -csv: CSV File with comma separated values of parameters \n";
        usage += "\t -dataType: XNAT Data type for which pipeline has been launched\n";
        usage += "\t -u: XNAT username \n";
        usage += "\t -pwd: XNAT password\n";
        usage += "\t -host: URL to XNAT based Website\n";
        usage += "\t -startAt: (optional) Step to start pipeline at -- \n";
        usage += "\t -notify: (optional) Email Ids to which notifications are to be sent\n";
        usage += "\t -noqueue: (optional) Launch the pipelines immediately. Default: Queue\n";
        usage += "\t -catalogPath: Root path relative to which Pipeline XML's are located\n";
        usage += "\t -continueOnException: Continue processing next session even if exception is encountered\n";
        usage += "\t -help\n";

        System.out.println(usage);
        System.exit(1);
    }
    
    /**
     * @return Returns the dataType.
     */
    public String getDataType() {
        System.out.println("Datatype is " + commandLineArgs.get("dataType"));
        return (String)commandLineArgs.get("dataType");
    }

    /**
     * @return Returns the CSV File Name.
     */
    public String getCSV() {
        return (String)commandLineArgs.get("csv");
    }

    
    /**
     * @return Returns the pipelineName.
     */
    public String getPipelineName() {
        return (String)commandLineArgs.get("pipeline");
    }

    public String getLogPropertiesFile() {
        return (String)commandLineArgs.get("log");
    }
    
    /**
     * @return Returns the startAt.
     */
    public String getStartAt() {
        return (String)commandLineArgs.get("startAt");
    }
    
    /**
     * @return Returns the host.
     */
    public String getHost() {
        return (String)commandLineArgs.get("host");
    }


    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return (String)commandLineArgs.get("password");
    }


    /**
     * @return Returns the userName.
     */
    public String getUserName() {
        return (String)commandLineArgs.get("username");
    }
    
    public ArrayList getEmailIds() {
        return ((ArrayList)commandLineArgs.get("notify"));
    }
    
    
    public Vector getFixedArguments() {
        return fixedArguments;
    }

    public boolean isQueued() {
        return !noqueue;
    }
    
    Hashtable commandLineArgs;
    Vector fixedArguments;
    int noOfRequiredArguments = 6;
    int noOfRequiredArgumentsAvailable = 0;
    boolean noqueue = false;
    boolean stopAtException = true;

    
}
