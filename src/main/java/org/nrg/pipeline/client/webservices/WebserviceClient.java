/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.webservices;

import org.nrg.xnattools.service.WebServiceClient;

public class WebserviceClient  {
	
	public static String CONNET_GET(String host, String uri, String uname, String passwd) {
		WebServiceClient client = new WebServiceClient(host, uname, passwd);
		String rtn = "";
		try {
			rtn = client.connect(uri);
		}catch(Exception e) {e.printStackTrace();}
		System.out.println("WebserviceClient GET Returned " + rtn);
		return rtn;
	}
	
}
