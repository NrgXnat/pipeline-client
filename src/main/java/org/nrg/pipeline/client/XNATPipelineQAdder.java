/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;


public class XNATPipelineQAdder {
    
    public boolean queue(String command, boolean queue) {
        if(queue)
            command = "arc-qadd " + command;
        boolean succeeded = true;
        try {
            Runtime.getRuntime().exec(command);
        }catch(Exception e) {
            System.out.println("XNATPipelineQAdder:: Couldnt launch " + command);
            succeeded = false;
        }
        return succeeded;
    }
}
