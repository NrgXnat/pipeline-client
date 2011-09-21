/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.xml.utils;

import javax.xml.namespace.QName;

import org.apache.xmlbeans.XmlCursor;
import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlOptions;
import org.nrg.pipeline.xmlbeans.ParametersDocument;

public class XMLUtils {
    
       
        public static String XmlToStringWithSuggestedPrefix(XmlObject xml, String host) {
            if (!host.endsWith("/")) host += "/";
            XmlOptions opts = new XmlOptions();
            opts.setSavePrettyPrint();
            opts.setSaveAggressiveNamespaces();
            
            String location = "http://nrg.wustl.edu/security  " + host + "schemas/security/security.xsd " ;
            location +=" http://nrg.wustl.edu/fs " + host + "schemas/cnda_xnat/fs.xsd ";
            location += "http://nrg.wustl.edu/behavioral " + host + "schemas/cnda_xnat/behavioral.xsd " ;
            location += "http://nrg.wustl.edu/ls2 " + host + "schemas/cnda_xnat/ls2.xsd ";
            location += "http://nrg.wustl.edu/xnat " + host + "schemas/xnat/xnat.xsd ";
            location += "http://nrg.wustl.edu/cnda " + host + "schemas/cnda_xnat/cnda_xnat.xsd ";
            location += "http://www.nbirn.net/prov " + host + "schemas/birn/birnprov.xsd";
            
            XmlCursor cursor = xml.newCursor();
            if (cursor.toFirstChild())
            {
              cursor.setAttributeText(new QName("http://www.w3.org/2001/XMLSchema-instance","schemaLocation"), location);
            }

            
            return xml.xmlText(opts);

        }
}
