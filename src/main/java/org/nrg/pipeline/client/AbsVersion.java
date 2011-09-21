/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

public abstract class AbsVersion {
    
    public void echoVersion() {
        Package p = this.getClass().getPackage();
        System.out.println("Implementation Version : " 
                + p.getImplementationVersion());
    }
}
