/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.drmaa.client;

import java.util.Map;

import org.nrg.pipeline.client.CommandLineArguments;
import org.nrg.pipeline.exception.PipelineException;
import org.nrg.pipeline.utils.PipelineUtils;
import org.nrg.pipeline.xmlbeans.PipelineData;
import org.nrg.pipeline.xmlbeans.PipelineData.ResourceRequirements.Property;
import org.nrg.pipeline.xmlbeans.PipelineDocument;
import org.nrg.pipeline.xmlbeans.PipelineData.ResourceRequirements;

public class PipelineResourceRequirements  {
	
	Property[] resources = null;
	String[] jobArgs = null;
	
	public static final String DRMAA_JOBTEMPLATE_JOBENVIRONMENT_PREFIX = "DRMAA_JobTemplate_JobEnvironment_";
	public static final String DRMAA_JOBTEMPLATE_JOBRESOURCE = "DRMAA_JobTemplate_JobResource";
	
	public PipelineResourceRequirements (String[] jobArgs) {
		this.jobArgs = jobArgs;
	}
	
	public void load() throws PipelineException {
		if (jobArgs != null && jobArgs.length > 0) {
	    	 CommandLineArguments cmdArgs = new CommandLineArguments(jobArgs);
	       	 String pathToPipelineXml = cmdArgs.getPipelineFullPath();
	       	PipelineDocument pipelineDoc = PipelineUtils.getPipelineDocument(pathToPipelineXml);
	       	PipelineData pipelineData = pipelineDoc.getPipeline();
	       	if (pipelineData.isSetResourceRequirements()) {
	       		ResourceRequirements resourceReq = pipelineData.getResourceRequirements();
	       		resources = resourceReq.getPropertyArray();
	       	}	
		}
	}
	
	public void setEnvironment(Map environment) {
		if (resources != null) {
	       		for (int i = 0; i < resources.length; i++) {
	   				String name = resources[i].getName();
	   				String jobEnvironmentName = null;
	       			if (name.startsWith(DRMAA_JOBTEMPLATE_JOBENVIRONMENT_PREFIX)) {
	       				String[] split = name.split("_");
	       				if (split.length == 4) {
	       					jobEnvironmentName = split[3];
	       				}else {
		       				for (int j = 3; j< split.length; j++) {
		       					jobEnvironmentName += split[j];
		       				}
	       				}
	       			}
	       			if (jobEnvironmentName != null) {
	       			   environment.put(jobEnvironmentName, resources[i].getStringValue());	
	       			}
	       		}
		 }
     }

	public String getResourceRequirements() {
		String rtn = null;
		if (resources != null) {
	       		for (int i = 0; i < resources.length; i++) {
	   				String name = resources[i].getName();
	       			if (name.startsWith(DRMAA_JOBTEMPLATE_JOBRESOURCE)) {
	       				rtn = resources[i].getStringValue();
	       				break;
	       			}
	       		}
		 }
		return rtn;
     }

	
}
