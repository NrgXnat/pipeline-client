/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;
import org.nrg.pipeline.drmaa.client.PipelineResourceRequirements;
import org.nrg.pipeline.exception.PipelineEngineException;

public class PipelineJobSubmitter {
	String command = null;
	String properties = null;
	String[]  jobArgs = null;
    String[] unescapedJobArgs = null;
    String pipeline_config_file = null;

    
	
	public PipelineJobSubmitter(String[] argv) {
	      // Initialize the command string and parameters to use for job
	      if (argv.length > 0) {		  // Arguments specified, use them
	    	  if (argv[0].endsWith("pipeline.config")) {
	    		  pipeline_config_file = argv[0];
	    		  command = argv[1];
		         jobArgs = new String[argv.length-2];
		         System.arraycopy(argv, 2, jobArgs, 0, argv.length-2);
	    	  }else {
	    	     command = argv[0];
		         jobArgs = new String[argv.length-1];
		         System.arraycopy(argv, 1, jobArgs, 0, argv.length-1);
	    	  }
		      unescapedJobArgs = new String[jobArgs.length];
	    	  System.arraycopy(jobArgs, 0, unescapedJobArgs, 0, jobArgs.length);

	      }
	}
	
	private boolean isXnatPipelineLauncher() {
		boolean rtn = false;
		rtn = command.toUpperCase().endsWith( "XNATPIPELINELAUNCHER");
		if (!rtn)
		 rtn = command.toUpperCase().endsWith(File.separator + "XNATPIPELINELAUNCHER");
		return rtn;	
	}
	
    private String getDefaultQueue() {
        String default_queue = null;
    	if (pipeline_config_file != null) {
	        Properties properties = new Properties();
	    	try {
	            properties.load(new FileInputStream(pipeline_config_file));
		        if (properties.size() == 0) {
		            System.out.println("Configuration file " +  pipeline_config_file +" doesnt have any properties specified .... aborting");
		            System.exit(1);
		        }
		        default_queue = properties.getProperty("DEFAULT_QUEUE");
		        
	    	}catch(Exception e){
	            System.out.println("Couldnt read the properties file " + pipeline_config_file +"....aborting" );
	            System.exit(1);
	    	}
    	}
        return default_queue;
    }

    private String getJobLogFile() {
        String log_file = null;
    	if (pipeline_config_file != null) {
	        Properties properties = new Properties();
	    	try {
	            properties.load(new FileInputStream(pipeline_config_file));
		        if (properties.size() == 0) {
		            System.out.println("Configuration file " +  pipeline_config_file +" doesnt have any properties specified .... aborting");
		            System.exit(1);
		        }
		        log_file = properties.getProperty("JOB_LOG_FILE");
	    	}catch(Exception e){
	            System.out.println("Couldnt read the properties file " + pipeline_config_file +"....aborting" );
	            System.exit(1);
	        }
    	}
        return log_file;
    }
	
	private void setupJob(JobTemplate jt) throws DrmaaException, PipelineEngineException{
        if (isXnatPipelineLauncher()) {
				PipelineResourceRequirements pipelineRes = new PipelineResourceRequirements(unescapedJobArgs);
				pipelineRes.load();
				setEnvironment(jt,pipelineRes);
				setResourceRequirements(jt, pipelineRes);
        }
	}
	
	private void setEnvironment(JobTemplate jt, PipelineResourceRequirements pipelineRes) throws DrmaaException, PipelineEngineException {
	        	Map jobEnvironment = jt.getJobEnvironment();
	        	if (jobEnvironment == null) {
	        		jobEnvironment = new Hashtable();
	        	}
	        	pipelineRes.setEnvironment(jobEnvironment);
	        	jt.setJobEnvironment(jobEnvironment);
     }
	
	private void setResourceRequirements(JobTemplate jt, PipelineResourceRequirements pipelineRes) throws DrmaaException, PipelineEngineException {
	        	String resourceReq = jt.getNativeSpecification();
	        	String pipelineResourceReq = pipelineRes.getResourceRequirements();
	        	String pipelineJobCategory = pipelineRes.getJobCategory();
	        	String qsub_options = "  -shell y -V ";
	        	String default_queue = getDefaultQueue();
	        	if (pipelineJobCategory != null) {
	        		String old = jt.getJobCategory();
	        		jt.setJobCategory(pipelineJobCategory);
	        		System.out.println("Job Category is " + jt.getJobCategory() + " OLD " + old);
	        	}
	        	if (pipelineResourceReq != null ) {
	        		if (!pipelineResourceReq.contains("-q ")) {
	        			if (default_queue != null) {
	        				qsub_options += " -q "  + default_queue + " "; 
	        			}
	        		}
	        		if (resourceReq != null) {
	        			jt.setNativeSpecification(resourceReq + " " + qsub_options + " " + pipelineResourceReq );
	        		}else 
	        			jt.setNativeSpecification( qsub_options + " " + pipelineResourceReq);
	        	}else {
	        		if (resourceReq != null) {
	        			//jt.setNativeSpecification(resourceReq + " -l arch=sol-sparc64" + qsub_options);
	        			jt.setNativeSpecification(resourceReq + " " + qsub_options);
	        		}else 
	        			//jt.setNativeSpecification(" -l arch=sol-sparc64" + qsub_options);
	        			jt.setNativeSpecification(" " + qsub_options);
	        	}
	        	
	        	System.out.println("Native host spec " + jt.getNativeSpecification());
	}
	

	 public static String now() {
		 final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
		    Calendar cal = Calendar.getInstance();
		    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		    return sdf.format(cal.getTime());

		  }
	
	private void log(String message) {
		String log_file = getJobLogFile();
		if (log_file != null) {
			System.out.println("LOGGING TO FILE " + log_file);
			try {
			 FileWriter fstream = new FileWriter(log_file, true);
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write("\n Date " + now() + " ---------------------- \n");
			  out.write(message);
			  out.write("\n -------------------------------------- \n");

			  //Close the output stream
			  out.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}else {
			System.out.println(pipeline_config_file + " JOB_LOG_FILE " + log_file + " is null");
		}
	}
	
	public int run() {
		int status = 1;
		String log_message = "";
        CommandLineArguments commandArgs = new CommandLineArguments(unescapedJobArgs);

		if (command != null) {
		      // Prepare for job run
		      SessionFactory factory = SessionFactory.getFactory();
		      Session session = factory.getSession();
		      JobTemplate jt = null;
		      try {
		         session.init(null);
		         jt = session.createJobTemplate();
		         String wd = java.lang.System.getProperty("out.location");
		         String cwd = java.lang.System.getProperty("cwd.location");
		         String aliasHost = null;
		         aliasHost = java.lang.System.getProperty("aliasHost");
		         if (cwd != null) {
		        	 jt.setWorkingDirectory(cwd);
		         }

		         jt.setRemoteCommand(command);
		          
		         log_message += "Command " + command + "\n";

		         if (jobArgs != null ) {
		        	 if (isXnatPipelineLauncher()) {
		        		 //Escape the password field
			        	 for (int i = 0; i < jobArgs.length; i++) {
			        		 if (jobArgs[i].equals("-pwd")) {
			        			 String prev = jobArgs[i+1]; 
			        			 jobArgs[i+1] = escapeSpecialShellCharacters(jobArgs[i+1]);
			        			 break;
			        		 }
			        	 }
		        	 }
		        	 jt.setArgs(Arrays.asList(jobArgs));
		         }

		         setupJob(jt);
		         String id = null;
		         if (isXnatPipelineLauncher()) {
		        	 //Hold the job get the job id. Insert that in the workflow table
		        	 jt.setJobName("J_"+commandArgs.getId());
		        	 jt.setJobSubmissionState(JobTemplate.HOLD_STATE);
		        	 log_message += "JOB_NAME " + jt.getJobName() + "\n";
		        	 log_message += "JOB_COMMAND " + command + "\n";
		        	 
		        	 for (int z=0; z< jobArgs.length;z++) {
		        	   log_message += "JOB_ARGS " + z + ":" + jobArgs[z] +"\n";
		        	 }
			         // Submit job to run & wait for completion
			         id = session.runJob(jt);
			         if (wd != null) {
			        	 jt.setOutputPath(wd);
			        	 jt.setErrorPath(wd);
			         }
			         System.out.println(" Job " + id + " is on HOLD. Assigning jobId to workflow");
			         
			         log(log_message +"\n job id= "+ id);
			         //Update the workflow table to insert the grid job id
			         if (aliasHost != null)
					      new XNATPipelineLauncher(commandArgs).assignGridJobIdToWorkflow(id, aliasHost);
		        	 else
		        		 new XNATPipelineLauncher(commandArgs).assignGridJobIdToWorkflow(id);
			         
			         System.out.println("Releasing the hold on  Job " + id);
			         System.out.print("Scheduling Command: " + command + " ");
			         for (int i = 0; i < jobArgs.length; i++)
			        	 System.out.print(jobArgs[i] + "  " );
			         System.out.println();
		 			 session.control(id, Session.RELEASE);
		         }else {
			         // Submit job to run & wait for completion
			         id = session.runJob(jt);
		         }
		         status = 0;
		      } catch (DrmaaException e) {
		         System.out.println ("DRMAA Error: " + e.getClass() + " " + e.getMessage());
		      }catch (PipelineEngineException pe) {
		         System.out.println ("Error: Possibly couldnt read the pipeline for resource requirements " + pe.getLocalizedMessage());
		      }finally {
			         // Cleanup after run
		    	    if (session != null) {
		    	    	try {
					         session.deleteJobTemplate(jt);
					         session.exit ();
		    	    	}catch(DrmaaException e) {
		    	    		System.out.println ("DRMAA Error: " + e.getClass() + " " + e.getMessage());
		    	    	}
		    	    }
		      }
		}else 
			showUsage();
		return status;
	}
	

	 private String escapeSpecialShellCharacters(String input) {
         String userShell = null;
         userShell = java.lang.System.getProperty("userShell");
         System.out.println("User Shell is " + userShell);
         if (userShell != null) {
        	 if (userShell.equalsIgnoreCase("SH")  ) {
        		  return escapeSpecial_Sh_ShellCharacters(input);
        	 }else if (userShell.equalsIgnoreCase("CSH")) {
            	 return escapeSpecialCShellCharacters(input);
        	 }else {
        		 //Unknown Shell Not supported
        		 System.out.println("Couldnt escape characters in the shell " + userShell);
        		 //throw new PipelineException("Couldnt escape characters in the shell " + userShell);
        		 return input;
        	 }
         }else {
        	 //Assumes default user shell to be sh
        	 return escapeSpecial_Sh_ShellCharacters(input);
         }

	 }
	
	private String escapeSpecial_Sh_ShellCharacters(String input) {
         String rtn=input;
         if (input == null) return rtn;
         if (!System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
//	         input = input.replace("\\", "\\\\");
	         String[] pieces = input.split("'");
	         	rtn = "";
	            for (int i=0; i < pieces.length; i++) {
	         	   rtn += "'" + pieces[i] + "'" + "\\'";
	            }
	            if (rtn.endsWith("\\'") && !input.endsWith("'") ) {
	         	   int indexOfLastQuote = rtn.lastIndexOf("\\'");
	         	   if (indexOfLastQuote != -1)
	         	     rtn = rtn.substring(0,indexOfLastQuote);
	            }
	           // rtn = rtn.replace("!", "\\!");
	            //Ref: http://steve-parker.org/sh/escape.shtml
//	            rtn = rtn.replace("$", "\\$");
//	            rtn = rtn.replace("`", "\\`");
//		        rtn = rtn.replace("\"", "\\\"");
         }
         return rtn;
    }

/*	private String escapeSpecial_BASH_ShellCharacters(String input) {
        String rtn=input;
        if (input == null) return rtn;
        //Ref: http://linux.die.net/Bash-Beginners-Guide/sect_03_03.html
        if (!System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
	         input = input.replace("\\", "\\\\");
		     input = input.replace("\"", "\\\"");

	         String[] pieces = input.split("'");
	         	rtn = "";
	            for (int i=0; i < pieces.length; i++) {
	         	   rtn += "\"" + pieces[i] + "\"" + "\\'";
	            }
	            if (rtn.endsWith("\\'") && !input.endsWith("'") ) {
	         	   int indexOfLastQuote = rtn.lastIndexOf("\\'");
	         	   if (indexOfLastQuote != -1)
	         	     rtn = rtn.substring(0,indexOfLastQuote);
	            }
	            rtn = rtn.replace("!", "\\!");
	            //Ref: http://steve-parker.org/sh/escape.shtml
	            rtn = rtn.replace("$", "\\$");
	            rtn = rtn.replace("`", "\\`");
        }
        return rtn;
   }*/
	
	
	 private String escapeSpecialCShellCharacters(String input) {
         String rtn=input;
         if (input == null) return rtn;
         if (!System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
         	String[] pieces = input.split("'");
         	rtn = "";
            for (int i=0; i < pieces.length; i++) {
         	   rtn += "'" + pieces[i] + "'" + "\\'";
            }
            if (rtn.endsWith("\\'") && !input.endsWith("'") ) {
         	   int indexOfLastQuote = rtn.lastIndexOf("\\'");
         	   if (indexOfLastQuote != -1)
         	     rtn = rtn.substring(0,indexOfLastQuote);
            }
            rtn = rtn.replace("!", "\\!");
         }
         return rtn;
    }
	
	private String getCommandName() {
		int i = command.lastIndexOf(File.separator);
		if (i != -1) {
			return command.substring(i+1);
		}else
		   return command;
	}
	
	public void showUsage() {
		System.out.println("PURPOSE: This java based utility will post jobs on the DRMAA API compliant grid ");
		System.out.println("USAGE: PipelineJobSubmitter XnatPipelineLauncher <options>");
	}
	
	public static void main (String[] argv) {
			PipelineJobSubmitter jsub = new PipelineJobSubmitter(argv);
			int status = jsub.run();
			System.exit(status);
	   }

}
