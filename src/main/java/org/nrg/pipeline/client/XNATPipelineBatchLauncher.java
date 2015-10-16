/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import org.apache.axis.client.Service;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.nrg.pipeline.exception.PipelineException;
import org.nrg.pipeline.utils.MailUtils;

import com.Ostermiller.util.CSVParser;
import com.Ostermiller.util.LabeledCSVParser;

public class XNATPipelineBatchLauncher  {

    public XNATPipelineBatchLauncher(BatchCommandLineArguments args) {
        commandLineArgs = args;
    }
    
    public void launch() throws IOException, PipelineException {
        LabeledCSVParser lcsvp = new LabeledCSVParser(new CSVParser(new BufferedReader( new FileReader(commandLineArgs.getCSV()) )));
        String[] labels = lcsvp.getLabels();
        while (lcsvp.getLine() != null) {
            Vector args = (Vector)commandLineArgs.getFixedArguments().clone();
            if(labels[0].equalsIgnoreCase("id")) {
                args.add("-id");
                args.add(lcsvp.getValueByLabel(labels[0]));
            }else {
                System.out.println("Expected to see the first col in " +  commandLineArgs.getCSV() + " to contain ID\n");
                printCSVFormat();
                throw new PipelineException("Incorrect cols in CSV file. Expect the first col in " + commandLineArgs.getCSV() + " to contain ID");
            }
            for (int i = 1; i < labels.length; i++) {
                args.add("-parameter");
                args.add(labels[i] + "=" + lcsvp.getValueByLabel(labels[i]));
            }
            String[] argv = new String[args.size()];
            args.copyInto(argv);
            
            CommandLineArguments commandArgs = new CommandLineArguments(argv);

            
            if (!commandLineArgs.isQueued()) {
                try {
                    XNATPipelineLauncher launcher = new XNATPipelineLauncher(commandArgs);
                    launcher.notify(false);
                    launcher.launch();
                }catch(Exception e) {
                    if (commandLineArgs.isStopAtException()) {
                        throw new PipelineException("Couldnt launch pipeline for id = " + lcsvp.getValueByLabel(labels[0]));
                    }else {
                        handleFailure(argv,e);
                    }
                }
            }else {
                String command = "XNATPipelineLauncher ";
                for (String commandLineComponent : argv) {
                    command += commandLineComponent;
                }
                command += " -supressNotification ";
                if (!new XNATPipelineQAdder().queue(command, true)) {
                    try {
                        MailUtils.send("Failure to Launch Build ","Couldnt not launch " + command,commandLineArgs.getEmailIds(), null, null);
                        System.exit(3);
                    }catch(Exception e1) {
                        System.out.println("Unable to send email about failure to launch CSV command");
                        System.exit(4);
                    }
                }
            }
        }
    }
    
    private void handleFailure(String msg, Exception e) {
        logger.info(msg,e);
    }
    
    private void handleFailure(String[] argv, Exception e) {
        String msg = "";
        if (argv != null) {
            for (int i = 0; i < argv.length; i++) {
                msg += " " + argv[i] + " ";
            }
        }
        handleFailure(msg,e);
    }
    
    
    public void printCSVFormat() {
        System.out.println("The first line of the CSV File supplied should contain the following columns");
        System.out.println("Column 1: <ID>");
        System.out.println("Column 2....onwards: <Pipeline Parameter name>");
        System.out.println("The remaining rows of the csv file should contain data for the columns");
        System.out.println("For a paramater which takes in multiple values, say A B C, insert data as \"A,B,C\"");
    }

    
    public static void main(String argv[]) {
        BatchCommandLineArguments commandArgs = new BatchCommandLineArguments(argv);
        if (commandArgs.getLogPropertiesFile() != null) {
            PropertyConfigurator.configure(commandArgs.getLogPropertiesFile());
        }else {
            BasicConfigurator.configure();
        }
        XNATPipelineBatchLauncher launcher = new XNATPipelineBatchLauncher(commandArgs);
        try {
            launcher.launch();
            try {
                MailUtils.send("Batch Build completed successfully ","Entire batch " + commandArgs.getCSV() + " has been " + (commandArgs.isQueued()?" queued ":" launched ") + " for pipeline " + commandArgs.getPipelineName() ,commandArgs.getEmailIds(), null, null);
                System.exit(0);
            }catch(Exception e1) {
                System.out.println("Unable to send email about status of build for CSV file " + commandArgs.getCSV());
                System.exit(20);
            }
        }catch(IOException ioe) {
            System.out.println("Possibly Couldnt access file " + commandArgs.getCSV());
            System.exit(1);
        }catch(PipelineException e) {
            System.out.println("Couldnt launch " + commandArgs.getCSV() + " " + e.getMessage());
            System.exit(2);
        }
    }

    
    
    static Logger logger = Logger.getLogger(XNATPipelineBatchLauncher.class);
    BatchCommandLineArguments commandLineArgs;
    
    String service_session = null;
    Service service;
    
}
