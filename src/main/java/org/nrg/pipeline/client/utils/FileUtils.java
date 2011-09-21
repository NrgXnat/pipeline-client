/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.utils;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.nrg.pipeline.xmlbeans.xnat.DicomSeries;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource;
import org.nrg.pipeline.xmlbeans.xnat.ImageResourceSeries;
import org.nrg.pipeline.xmlbeans.xnat.MRSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.PETSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.ResourceCatalog;

public class FileUtils {
    
    public static boolean exists(String fileName) {
        boolean rtn = false;
        File dir = new File(fileName);
        if (dir.exists()) rtn = true;
        return rtn;
    }
    
 
    
    public static String getRelativePath(MRSessionDocument mrSession)  {
        String rtn = null;
        if (mrSession.getMRSession().getScans()==null || mrSession.getMRSession().getScans().sizeOfScanArray()==0) {
            System.out.println("No scans in the sessions");
            System.exit(2);
        }
        if (mrSession.getMRSession().getScans().getScanArray(0) == null || mrSession.getMRSession().getScans().getScanArray(0).sizeOfFileArray()==0) {
            System.out.println("No files associated with the first scan in the sessions");
            System.exit(3);
        }
        String uri = null;
        try {
            ImageResourceSeries img = (ImageResourceSeries)mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).changeType(mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).schemaType());
            uri = img.getPath();
        }catch (ClassCastException ce) {
            try {
                ResourceCatalog catalog = (ResourceCatalog)mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).changeType(mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).schemaType());
                uri = catalog.getURI();
            }catch(ClassCastException ce1) {
                try {
                    DicomSeries ds = (DicomSeries)mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).changeType(mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).schemaType());
                    uri = ds.getImageSet().getImageArray(0).getURI();
                }catch(ClassCastException ce2) {
                    ImageResource img = (ImageResource)mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).changeType(mrSession.getMRSession().getScans().getScanArray(0).getFileArray(0).schemaType());
                    uri = img.getURI();
                }
            }
        }
        if (uri == null) {
            System.out.println("Couldnt find the URI of the first scans first file");
            System.exit(4);
        }
        String sessionId = mrSession.getMRSession().getLabel();
        if (!uri.startsWith(sessionId)) {
            int indexOfSessionId = uri.indexOf("/" + sessionId + "/");
            if (indexOfSessionId != -1) {
                rtn = uri.substring(0,indexOfSessionId);
                if (!rtn.endsWith(File.separator)) rtn += File.separator;
            }
        }else {
            rtn = "";
        }
        return rtn;
    }
    
    
    public static String getRelativePath(PETSessionDocument petSession) {
        String rtn = null;
        if (petSession.getPETSession().getScans()==null || petSession.getPETSession().getScans().sizeOfScanArray()==0) {
            System.out.println("No scans in the sessions");
            System.exit(2);
        }
        if (petSession.getPETSession().getScans().getScanArray(0) == null || petSession.getPETSession().getScans().getScanArray(0).sizeOfFileArray()==0) {
            System.out.println("No files associated with the first scan in the sessions");
            System.exit(3);
        }
        String uri = null;
        try {
            ImageResourceSeries img = (ImageResourceSeries)petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).changeType(petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).schemaType());
            uri = img.getPath();
        }catch (ClassCastException ce) {
            try {
                DicomSeries ds = (DicomSeries)petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).changeType(petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).schemaType());
                uri = ds.getImageSet().getImageArray(0).getURI();
            }catch(ClassCastException ce1) {
                try {
                	ImageResource img = (ImageResource)petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).changeType(petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).schemaType());
                    uri = img.getURI();
                }catch(ClassCastException ce2) {
                	ResourceCatalog img = (ResourceCatalog)petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).changeType(petSession.getPETSession().getScans().getScanArray(0).getFileArray(0).schemaType());
                    uri = img.getURI();
                }
            }
        }
        if (uri == null) {
            System.out.println("Couldnt find the URI of the first scans first file");
            System.exit(4);
        }
        String sessionId = petSession.getPETSession().getLabel();
        if (!uri.startsWith(sessionId)) {
            int indexOfSessionId = uri.indexOf("/" + sessionId + "/");
            if (indexOfSessionId != -1) {
                rtn = uri.substring(0,indexOfSessionId);
                if (!rtn.endsWith(File.separator)) rtn += File.separator;
            }
        }else {
            rtn = "";
        }
        
        return rtn;
    }
    
    public static String getPath(String pipelinePath, String relativePath, String sessionId) {
        String rtn = pipelinePath;
        if (relativePath.endsWith("/")) relativePath = relativePath.substring(0, relativePath.length()-1);
        if (relativePath == null) return pipelinePath;
        int indexOfSessionId = pipelinePath.indexOf("/" + sessionId + "/");
        if (indexOfSessionId != -1) {
            rtn = StringUtils.replace(pipelinePath,pipelinePath.substring(0,indexOfSessionId),relativePath);
        }
        return rtn;
    }
    
    public static  void main(String args[]) {
    	String pipelinePath = "/data/nil-bluearc/marcus/CNDA_ACCESSORIES/build/NP933/20100815_181436/stdb/1020_89/PROCESSED/BOLD/boldrun1/1_brun1_faln_dbnd.4dfp.img";
    	String relativePath = "/data/nil-bluearc/marcus/CNDA/NP933/arc001/";
    	String sessionId = "1020_89";
    	System.out.println("Relative Path is " + FileUtils.getPath(pipelinePath, relativePath, sessionId));
    	System.out.println(relativePath.indexOf("nil")+ " " + relativePath.substring(0,6));
    	 
    	
    }
    
    public static String getTempFolder() {
		String rtn =  System.getProperty("pipeline.tmp");
		if (rtn == null) {
			rtn = System.getProperty("user.home");
		}
		return rtn;
	}  
    
}   
