/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import java.io.File;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;
import org.nrg.pipeline.drmaa.client.PipelineResourceRequirements;
import org.nrg.pipeline.exception.PipelineEngineException;
import org.nrg.pipeline.exception.PipelineException;

public class PipelineJobSubmitter {
	String command = null;
	String properties = null;
	String[]  jobArgs = null;
   String[] unescapedJobArgs = null; 

    
	
	public PipelineJobSubmitter(String[] argv) {
	      // Initialize the command string and parameters to use for job
	      if (argv.length > 0) {		  // Arguments specified, use them
	         command = argv[0];			  
	         jobArgs = new String[argv.length-1];
	         System.arraycopy(argv, 1, jobArgs, 0, argv.length-1);
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
	
	private void setupJob(JobTemplate jt) throws DrmaaException, PipelineEngineException{
        if (isXnatPipelineLauncher()) {
				PipelineResourceRequirements pipelineRes = new PipelineResourceRequirements(unescapedJobArgs);
				pipelineRes.load();
				setEnvironment(jt,pipelineRes);
				setResourceRequirements(jt, pipelineRes);
        }
	}
	
	@SuppressWarnings("rawtypes")
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
	        	String qsub_options = "  -shell y ";
	        	if (pipelineResourceReq != null ) {
	        		if (resourceReq != null) {
	        			jt.setNativeSpecification(resourceReq + " " + pipelineResourceReq + qsub_options);
	        		}else 
	        			jt.setNativeSpecification(pipelineResourceReq + qsub_options);
	        	}else {
	        		if (resourceReq != null) {
	        			jt.setNativeSpecification(resourceReq + " -l arch=sol-sparc64" + qsub_options);
	        		}else 
	        			jt.setNativeSpecification(" -l arch=sol-sparc64" + qsub_options);
	        	}
	        	System.out.println("Native host spec " + jt.getNativeSpecification());
	}
	
	
	public int run() {
		int status = 1;
		if (command != null) {
		      // Prepare for job run
		      SessionFactory factory = SessionFactory.getFactory();
		      Session session = factory.getSession();
		      JobTemplate jt = null;
		      try {
		         session.init(null);
		         jt = session.createJobTemplate();
		         String wd = java.lang.System.getProperty("out.location");
		         String aliasHost = null;
		         aliasHost = java.lang.System.getProperty("aliasHost");
		         if (wd != null) {
		        	 jt.setWorkingDirectory(wd);
		         }

		         jt.setRemoteCommand(command);
		         

		         if (jobArgs != null ) {
		        	 if (isXnatPipelineLauncher()) {
		        		 //Escape the password field
			        	 for (int i = 0; i < jobArgs.length; i++) {
			        		 if (jobArgs[i].equals("-pwd")) {
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

		        	 jt.setJobSubmissionState(JobTemplate.HOLD_STATE);
			         // Submit job to run & wait for completion
			         id = session.runJob(jt);
			         if (wd != null) {
			        	 jt.setOutputPath(wd);
			        	 jt.setErrorPath(wd);
			         }
			         System.out.println(" Job " + id + " is on HOLD. Assigning jobId to workflow");
			         
			         
			         //Update the workflow table to insert the grid job id
			         if (aliasHost != null)
					      new XNATPipelineLauncher(unescapedJobArgs).assignGridJobIdToWorkflow(id, aliasHost);
		        	 else
		        		 new XNATPipelineLauncher(unescapedJobArgs).assignGridJobIdToWorkflow(id);
			         
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
         if (userShell != null) {
        	 if (userShell.equalsIgnoreCase("SH")) {
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
         }
         return rtn;
    }
	 
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
