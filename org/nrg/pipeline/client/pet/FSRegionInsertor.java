/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.pet;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;
import java.io.FileFilter;
import java.util.Hashtable;

import org.apache.log4j.BasicConfigurator;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.xmlbeans.cnda.PETTimeCourseDocument;
import org.nrg.pipeline.xmlbeans.xnat.AbstractResource;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource;
import org.nrg.pipeline.xmlbeans.xnat.PETSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.RegionResource;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource.Dimensions;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource.VoxelRes;
import org.nrg.pipeline.xmlbeans.xnat.ImageSessionData.Regions;
import org.nrg.pipeline.xmlbeans.xnat.RegionResource.Creator;
import org.nrg.pipeline.xmlbeans.xnat.RegionResource.Subregionlabels;
import org.nrg.pipeline.xmlbeans.xnat.RegionResource.Subregionlabels.Label;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnattools.xml.XMLSearch;
import org.nrg.xnattools.xml.XMLStore;

public class FSRegionInsertor {
	
    Hashtable<String,String> commandLineArgs;
    PETTimeCourseDocument petTimeCourseDoc = null;
    PETSessionDocument petSession = null;
    int noOfRequiredArgumentsAvailable = 0;
    int noOfRequiredArguments = 6;
    String regionName_prefix = "FS_";
    
    public FSRegionInsertor(String args[]) {
        parseCommandLineArguments(args);
    }

  
	private void parseCommandLineArguments(String args[]) {
		commandLineArgs = new Hashtable<String,String>();
	    int c;
	    LongOpt[] longopts = new LongOpt[7];
	    longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
	    longopts[1] = new LongOpt("sessionId", LongOpt.REQUIRED_ARGUMENT, null, 's');
	    longopts[2] = new LongOpt("xnatId", LongOpt.REQUIRED_ARGUMENT, null, 'i');
	    longopts[3] = new LongOpt("dir", LongOpt.REQUIRED_ARGUMENT, null, 't');
        longopts[4] = new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'u');
	    longopts[5] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'w');
	    longopts[6] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o');
	    
	    //Getopt g = new Getopt("PETTimeSeriesAssessorCreator", args, "s:t:u:w:d:a:o:l:f:m:e:k:h;", longopts, true);
	    Getopt g = new Getopt("FSRegionInsertor", args, "s:i:t:u:w:o:h;", longopts, true);
	    g.setOpterr(false); // We'll do our own error handling
	    //
	    while ((c = g.getopt()) != -1) {
	      switch (c)
	        {
	            case 's':
	               commandLineArgs.put("sessionId",g.getOptarg());
	               noOfRequiredArgumentsAvailable++;
	               break;
	            case 'i':
		               commandLineArgs.put("xnatId",g.getOptarg());
		               noOfRequiredArgumentsAvailable++;
		               break;
	            case 't':
	                commandLineArgs.put("dir",g.getOptarg());
	                noOfRequiredArgumentsAvailable++;
	                break;
	            case 'u':
	                commandLineArgs.put("username",g.getOptarg());
	                noOfRequiredArgumentsAvailable++;
	                break;
	            case 'w':
	                commandLineArgs.put("password",g.getOptarg());
	                noOfRequiredArgumentsAvailable++;
	                break;
	            case 'o':
	                String host = g.getOptarg();
	                if (!host.endsWith("/")) host+="/";
	                commandLineArgs.put("host",host);
	                noOfRequiredArgumentsAvailable++;
	                break;
	            case 'h':
	                printUsage();
	              break;
	            default:
	              printUsage();
	              break;               
	        }
	    }
	    if (noOfRequiredArgumentsAvailable < noOfRequiredArguments) {
	        System.out.println("Missing required arguments");
	        printUsage();
	    }
	}
	
	
	public void insertRegion() throws Exception {
		setPetSessionFromHost();
		//Get the files in the folder which look like <SESSION_ID>_fs_*_8bit.img
		File roiFolder = new File(commandLineArgs.get("dir"));
		if (roiFolder.exists() && roiFolder.isDirectory()) {
			FileFilter filter = new FSFileFilter("R");
			File[] RRegionFiles = roiFolder.listFiles(filter);
			filter = new FSFileFilter("L");
			File[] LRegionFiles = roiFolder.listFiles(filter);
			filter = new FSFileFilter("Both");
			File[] BothRegionFiles = roiFolder.listFiles(filter);
			if (BothRegionFiles.length == 0 && RRegionFiles.length == 0 && LRegionFiles.length ==0 ) 
				handleError(commandLineArgs.get("dir") + " doesnt contain files of the pattern " + commandLineArgs.get("sessionId") +"_fs_*_{R|L}_8bit.img");
			Regions regions = null;
			if (petSession.getPETSession().isSetRegions()) {
				regions = petSession.getPETSession().getRegions();
			}else {
				regions = petSession.getPETSession().addNewRegions();
			}
			addRegion(RRegionFiles, regions, RegionResource.Hemisphere.RIGHT, Label.Hemisphere.RIGHT, "_R", 2);
			addRegion(LRegionFiles, regions, RegionResource.Hemisphere.LEFT, Label.Hemisphere.LEFT, "_L", 1);
			addRegion(BothRegionFiles, regions, RegionResource.Hemisphere.BOTH, Label.Hemisphere.BOTH, "_B", 1);
			try {
			//	petSession.save(new File(petSession.getPETSession().getID()+".xml"), new XmlOptions().setSavePrettyPrint());
               	new XMLStore(petSession,commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).store();
			}catch(Exception e) {
				handleError("Couldnt store the PetSession");
			}
		}else {
			handleError(commandLineArgs.get("dir") + " is either not a folder or doesnt exist" );
		}
		
	}
	
	private void setPetSessionFromHost() throws Exception {
        String createdFile = new XMLSearch(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).searchFirst("xnat:petSessionData.ID",commandLineArgs.get("xnatId"), "=","xnat:petSessionData",FileUtils.getTempFolder());
        petSession = (PETSessionDocument)new XmlReader().read(createdFile, true);
   }

	private void addRegion(File[] regionFiles, Regions regions, RegionResource.Hemisphere.Enum region_hemi, Label.Hemisphere.Enum label_hemi, String hemi_code, int label_val) {
		if (regionFiles==null) return;
		String archiveRoot = getArchiveRoot() ;
		String roi_root = archiveRoot + File.separator + "ROIS" + File.separator;
		for (int i = 0; i < regionFiles.length; i++) {
			File f = regionFiles[i];
			String regionName = getRegionName(f);
			RegionResource region = regions.addNewRegion();
			region.setName(regionName_prefix + regionName);
			region.setHemisphere(region_hemi);
			region.setSessionId(commandLineArgs.get("sessionId"));
			ImageResource file = ImageResource.Factory.newInstance();
			file.setURI(roi_root  + f.getName());
			file.setFormat("ANALYZE");
			file.setContent("Freesurfer segmented region");
			Dimensions dim = file.addNewDimensions();
			dim.setX(new java.math.BigInteger("128"));
			dim.setY(new java.math.BigInteger("128"));
			dim.setZ(new java.math.BigInteger("75"));
			dim.setVolumes(new java.math.BigInteger("1"));
			VoxelRes vox = file.addNewVoxelRes();
			vox.setX(2); vox.setY(2); vox.setZ(2);
			file.setOrientation("Transverse");
			region.setFile(file);
			ImageResource baseImage = ImageResource.Factory.newInstance();
			baseImage.setURI(archiveRoot+ File.separator + "ASSESSORS"  + File.separator + "FREESURFER" + File.separator + "mri" + File.separator + "aparc+aseg.mgz");
			baseImage.setFormat("MGZ");
			baseImage.setContent("Freesurfer APARC+ASEG file");
			Dimensions basedim = baseImage.addNewDimensions();
			basedim.setX(new java.math.BigInteger("256"));
			basedim.setY(new java.math.BigInteger("256"));
			basedim.setZ(new java.math.BigInteger("256"));
			basedim.setVolumes(new java.math.BigInteger("1"));
			VoxelRes voxb = baseImage.addNewVoxelRes();
			voxb.setX(1); voxb.setY(1); voxb.setZ(1);
			region.setBaseimage(baseImage);
			Creator creator = region.addNewCreator();
			creator.setFirstname("Freesurfer");
			creator.setLastname("Segmented");
			Subregionlabels slabels = region.addNewSubregionlabels();
			Label label = slabels.addNewLabel();
			label.setId(label_val);
			label.setHemisphere(label_hemi);
			label.setStringValue(regionName_prefix + regionName + hemi_code);
		}
	}
	
	private String getArchiveRoot() {
		AbstractResource absRsc = petSession.getPETSession().getScans().getScanArray(0).getFileArray(0);
        ImageResource rsc = (ImageResource)absRsc.changeType(ImageResource.type);
        String path = rsc.getURI();
        int index = path.indexOf("/RAW/");
        return path.substring(0, index);
	}
	
	private String getRegionName(File regionFile) {
		String name = regionFile.getName();
		String pattern = commandLineArgs.get("sessionId") + "_fs_";
		String[] pieces = name.split(pattern);
		String[] spieces = pieces[1].split("_");
		return spieces[0];
	}
	
	private void handleError(String msg) {
		System.out.println(msg);
		System.exit(1);
	}
	
	private class FSFileFilter implements java.io.FileFilter {
		String lr ;
		public FSFileFilter(String region) {
			lr=region;
		}
	    public boolean accept(File f) {
	        String name = f.getName();
	        String pattern = commandLineArgs.get("sessionId") + "_fs_[a-zA-Z-]+_"+ lr +"_8bit\\.img"; 
	        return  name.matches(pattern);
	    }//end accept
	}//end class FSFileFilter
	
	  public void printUsage() {
	        String usage = "FSRegionInsertor  \n";
	        usage += "Options:\n";
	        usage += "\t -sessionId <session label>\n";
	        usage += "\t -xnatId <session XNAT id>\n";
	        usage += "\t -dir: <Dir containing FS segmented ROI's> \n";
	        usage += "\t -u: XNAT username [Optional: will parse .xnatPass file] \n";
	        usage += "\t -pwd: XNAT password [Optional: will parse .xnatPass file]\n";
	        usage += "\t -host: URL to XNAT based Website\n";
	        usage += "\t -help\n";
	        System.out.println(usage);
	        System.exit(1);
	    }
	  
	  public static void main(String args[]) {
		  FSRegionInsertor insertor = new FSRegionInsertor(args);
           BasicConfigurator.configure();
	      try {
	    	  insertor.insertRegion();
	            System.exit(0);
	    	  //String name = "test_p7174_fs_Cerebral-Cortex_R_8bit.img" ;
	    	  //String pattern = "test_p7174" + "_fs_[a-zA-Z-]+_R_8bit\\.img";
	    	  //System.out.println(pattern + " " + name.matches(pattern));
	      }catch (Exception e) {
	    	  System.out.println("Encountered " + e.getMessage());
	    	  e.printStackTrace();
	    	  System.exit(1);
	      }
	    }

}
