/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xdat.bean.CatDcmcatalogBean;
import org.nrg.xdat.bean.CatEntryBean;
import org.nrg.xdat.bean.XnatAbstractresourceBean;
import org.nrg.xdat.bean.XnatDicomseriesBean;
import org.nrg.xdat.bean.XnatDicomseriesImageBean;
import org.nrg.xdat.bean.XnatImageresourceBean;
import org.nrg.xdat.bean.XnatImagescandataBean;
import org.nrg.xdat.bean.XnatImagesessiondataBean;
import org.nrg.xdat.bean.XnatResourceBean;
import org.nrg.xdat.bean.XnatResourcecatalogBean;
import org.nrg.xdat.bean.XnatResourceseriesBean;
import org.nrg.xnattools.xml.XMLSearch;

public class CopyDicomFiles {
    
  //  String sessionId = null;
    String xnatId = null;
    String scanId = null;
    String destinationDir = null;
    String host = null;
    String user = null;
    String pwd = null;
    int noOfRequiredArguments = 6;
    
    public CopyDicomFiles(String argv[]) {
        int c;
        
        int argsAvailable = 0;
        
        LongOpt[] longopts = new LongOpt[7];
        //longopts[0] = new LongOpt("sessionId", LongOpt.REQUIRED_ARGUMENT, null, 's'); 
        longopts[0] = new LongOpt("xnatId", LongOpt.REQUIRED_ARGUMENT, null, 'x'); 
        longopts[1] = new LongOpt("scan", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[2] = new LongOpt("destinationDir", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        longopts[3] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[4] = new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'u');
        longopts[5] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'p');
        longopts[6] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        
        Getopt g = new Getopt("CopyDicomFiles", argv, "x:c:d:o:u:p:h;", longopts, true);
        g.setOpterr(false); // We'll do our own error handling
        //
        while ((c = g.getopt()) != -1) {
          switch (c)
            {
//                case 's':
//                    sessionId = g.getOptarg();
 //                   argsAvailable++;
  //                  break;
                case 'x':
                    xnatId = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'c':
                    scanId = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'd':
                    destinationDir = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'o':
                    host = g.getOptarg();
                    if (!host.endsWith("/")) host+="/";
                    argsAvailable++;
                    break;
                case 'u':
                    user = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'p':
                    pwd = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'h':
                     printUsage();
                     break;
                default:
                     printUsage();
                     break;
            }
        }
        if (argsAvailable < noOfRequiredArguments) {
            System.out.println("Missing required arguments");
            printUsage();
            System.exit(1);
        }
    }

    protected XnatImagesessiondataBean getImageSessionFromHost() throws Exception {
    	XnatImagesessiondataBean imageSession  = (XnatImagesessiondataBean) new XMLSearch(host, user, pwd).getBeanFromHost(xnatId, true);
        return imageSession;
   }
    
    private XnatAbstractresourceBean getScanFile(XnatImagescandataBean imagescan) {
    	XnatAbstractresourceBean file = null;
    	String scanType = imagescan.getType();
    	String rawScanContentCode = scanType + "_RAW";
    	ArrayList <XnatAbstractresourceBean> files = imagescan.getFile();
    	if (files.size() > 0) {
	    	if (files.size() == 1) {
	        	file = files.get(0);
	        }else  {
	        	file = getFileByContent(imagescan, rawScanContentCode);
	        	if (file == null)
	        		file = getFileByContent(imagescan, "RAW");
	        }
    	}
    	return file;
    }
    
    private XnatAbstractresourceBean getFileByContent( XnatImagescandataBean imageScan, String content) {
    	XnatAbstractresourceBean rtn = null;
    	try {
				for (int i = 0; i < imageScan.getFile().size(); i++) {
					XnatAbstractresourceBean f = imageScan.getFile().get(i);
				    if (f instanceof XnatResourceBean) {
				    	XnatResourceBean resource = (XnatResourceBean)f;
				    	if (resource.getContent().equals(content)) {
				    		rtn = f;
				    		break;
				    	}
				    }else if (f instanceof XnatImageresourceBean) {
				    	XnatImageresourceBean imageResource = (XnatImageresourceBean)f;
				    	if (imageResource.getContent().equals(content)) {
				    		rtn = f;
				    		break;
				    	}
				    }else if (f instanceof XnatResourceseriesBean) {
				    	XnatResourceseriesBean resourceSeries = (XnatResourceseriesBean)f;
				    	if (resourceSeries.getContent().equals(content)) {
				    		rtn = f;
				    		break;
				    	}
				    }else if (f instanceof XnatDicomseriesBean) {
				    	XnatDicomseriesBean resourceSeries = (XnatDicomseriesBean)f;
				    	if (resourceSeries.getContent().equals(content)) {
				    		rtn = f;
				    		break;
				    	}
				    }else if (f instanceof XnatResourcecatalogBean) {
				    	XnatResourcecatalogBean resourceCat = (XnatResourcecatalogBean)f;
				    	if (resourceCat.getContent().equals(content)) {
				    		rtn = f;
				    		break;
				    	}
				    }
					
				}
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    	return rtn;
    }
    
   public void copyScanFiles() {
       try {
           XnatImagesessiondataBean imageSession = getImageSessionFromHost();
           boolean found = false;
           ArrayList<XnatImagescandataBean> imageScans = imageSession.getScans_scan(); 
           if (imageScans != null && imageScans.size() > 0) {
               for (int i = 0; i < imageScans.size(); i++) {
                   XnatImagescandataBean mrscan = imageScans.get(i); 
                   if (mrscan.getId().equals(scanId)) {
                       if (mrscan.getFile().size() > 0) {
                    	   XnatAbstractresourceBean file = getScanFile(mrscan);
                           if (file instanceof XnatResourcecatalogBean) {
                        	   XnatResourcecatalogBean catalog = (XnatResourcecatalogBean)file;
                               String catalogPath = catalog.getUri();
                               if (catalogPath.endsWith("/")) catalogPath = catalogPath.substring(0,catalogPath.length()-1);
                               CatDcmcatalogBean dcmCatalogBean =  (CatDcmcatalogBean)new XmlReader().getBeanFromXml(catalogPath, false);
                               ArrayList<CatEntryBean>  catalogEntries = dcmCatalogBean.getEntries_entry();
                               int lastIndexOfSlash = catalogPath.lastIndexOf("/");
                               if (lastIndexOfSlash != -1) catalogPath = catalogPath.substring(0,lastIndexOfSlash);
                               if (catalogEntries.size() > 0) {
                                   for (int j = 0; j < catalogEntries.size(); j++) {
                                       String uri = catalogEntries.get(j).getUri();
                                       copyFile(catalogPath+"/"+uri);
                                   }
                               }
                           }else {
                                   XnatDicomseriesBean dicoms = (XnatDicomseriesBean)file;
                                   ArrayList<XnatDicomseriesImageBean>  imageSet = dicoms.getImageset_image();
                                   for (int k = 0; k < imageSet.size(); k++) {
                                       String uri = imageSet.get(k).getUri();
                                       copyFile(uri);
                                   }
                            }
                           found = true;
                           break;
                       }
                   }
               }
           }else {
               System.out.println("No DICOM scans available for this session");
               System.exit(1);
           }
           if (!found) {
               System.out.println("Couldnt find any DICOM files ");
           }
       }catch(Exception e) {
           e.printStackTrace();
           System.exit(1);
       }
       
   }
  
   private void copyFile(String uri) throws Exception {
       String filePath =  uri;
       File f = new File(filePath);
       if (!f.exists()) {
           System.out.println("File " + f.getAbsolutePath() + " doesnt exsit. Will try to looked for zipped file");
           f = new File(filePath +  ".gz");
       }
       if (f.exists()) {
           copyFile(f, new File(destinationDir + File.separator + f.getName()));
       }
   }
   

   
   public void copyFile(File in, File out) throws Exception {
       FileInputStream fis  = new FileInputStream(in);
       FileOutputStream fos = new FileOutputStream(out);
       byte[] buf = new byte[1024];
       int i = 0;
       while((i=fis.read(buf))!=-1) {
         fos.write(buf, 0, i);
         }
       fis.close();
       fos.close();
   }

  
    public void printUsage() {
        System.out.println("Usage: CopyDicomFiles options");
        System.out.println("-h<help>"); 
        System.out.println("-sessionId <MR Session Label> - DEPRECATED");
        System.out.println("-xnatId <XNAT MR Session ID>"); 
        System.out.println("-scan <The scan whose files are to be obtained>");
        System.out.println("-destinationDir <The directory where the files of the scan are to be copied>");
        System.out.println("-host <URL to the XNAT Site>");
        System.out.println("-u <XNAT User id >");
        System.out.println("-pwd <XNAT Password >");
    }
    
    public static void main(String args[]) {
        try {
            BasicConfigurator.configure();
            CopyDicomFiles copy = new CopyDicomFiles(args);
            copy.copyScanFiles();
            System.exit(0);
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }    
    
}
