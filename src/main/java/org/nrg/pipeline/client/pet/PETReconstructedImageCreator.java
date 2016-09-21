/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.pet;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import ij.io.FileInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.axis.client.Service;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlOptions;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.exception.PipelineException;
import org.nrg.pipeline.utils.XMLBeansUtils;
import org.nrg.pipeline.xmlbeans.AllResolvedStepsDocument;
import org.nrg.pipeline.xmlbeans.OutputData;
import org.nrg.pipeline.xmlbeans.ParameterData;
import org.nrg.pipeline.xmlbeans.ResolvedStepDocument.ResolvedStep;
import org.nrg.pipeline.xmlbeans.ResolvedStepDocument.ResolvedStep.Provenance;
import org.nrg.pipeline.xmlbeans.ResolvedStepDocument.ResolvedStep.ResolvedOutput;
import org.nrg.pipeline.xmlbeans.xnat.ComputationData;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource;
import org.nrg.pipeline.xmlbeans.xnat.ImageScanData;
import org.nrg.pipeline.xmlbeans.xnat.ImageSessionData.Reconstructions;
import org.nrg.pipeline.xmlbeans.xnat.PETSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.PetScanData;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData;
import org.nrg.pipeline.xmlbeans.xnat.Resource;
import org.nrg.pipeline.xmlbeans.xnat.ExperimentData.Fields;
import org.nrg.pipeline.xmlbeans.xnat.ExperimentData.Fields.Field;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource.Dimensions;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource.VoxelRes;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData.Computations;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData.In;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData.InScans;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData.Out;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnat.plexiviewer.reader.PlexiImageHeaderReader;
import org.nrg.xnattools.XNATPassFileParser;
import org.nrg.xnattools.exception.UserNameNotFoundException;
import org.nrg.xnattools.xml.XMLSearch;
import org.nrg.xnattools.xml.XMLStore;

import com.Ostermiller.util.CSVParser;
import com.Ostermiller.util.LabeledCSVParser;

public class PETReconstructedImageCreator {
    
    public PETReconstructedImageCreator(String argv[]) {
        int c;
        commandLineArgs = new Hashtable<String,String>();
        inScanIds = new ArrayList<String>();
        stepIds = new ArrayList<String>();
        stepIndices = new Hashtable<String, ArrayList<Integer>>();
        LongOpt[] longopts = new LongOpt[17];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[1] = new LongOpt("sessionId", LongOpt.REQUIRED_ARGUMENT, null, 's'); 
        longopts[2] = new LongOpt("xnatId", LongOpt.REQUIRED_ARGUMENT, null, 'i'); 
        longopts[3] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');
        longopts[4] = new LongOpt("baseScanType", LongOpt.REQUIRED_ARGUMENT, null, 'b');
        longopts[5] = new LongOpt("scan", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[6] = new LongOpt("scanParameterName", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        longopts[7] = new LongOpt("stepId", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        longopts[8] = new LongOpt("pipelineXml", LongOpt.REQUIRED_ARGUMENT, null, 'x');
        longopts[9] = new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'u');
        longopts[10] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'w');
        longopts[11] = new LongOpt("buildDir", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        longopts[12] = new LongOpt("archiveDir", LongOpt.REQUIRED_ARGUMENT, null, 'a');
        longopts[13] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[14] = new LongOpt("log", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        longopts[15] = new LongOpt("computation", LongOpt.REQUIRED_ARGUMENT, null, 'p');
        longopts[16] = new LongOpt("mpragesessionid", LongOpt.REQUIRED_ARGUMENT, null, 'm');
        
        Getopt g = new Getopt("PETReconstructedImageCreator", argv, "s:i:t:b:c:n:f:x:u:w:d:a:o:l:p:m:h;", longopts, true);
        g.setOpterr(false); // We'll do our own error handling
        //
        while ((c = g.getopt()) != -1) {
          switch (c)
            {
                case 'l':
                    commandLineArgs.put("log",g.getOptarg());
                    break;
                case 'p':
                    commandLineArgs.put("computation",g.getOptarg());
                    break;
                case 's':
                   commandLineArgs.put("sessionId",g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
                case 'i':
                    commandLineArgs.put("xnatId",g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 't':
                   commandLineArgs.put("type",g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'b':
                   commandLineArgs.put("baseScanType",g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'c':
                   inScanIds.add(g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'n':
                   commandLineArgs.put("scanParameterName",g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'f':
                   stepIds.add(g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'x':
                   commandLineArgs.put("pipelineXml",g.getOptarg());
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'u':
                   commandLineArgs.put("username",g.getOptarg());
                   //noOfRequiredArgumentsAvailable++;
                   break;
               case 'w':
                   commandLineArgs.put("password",g.getOptarg());
                   //noOfRequiredArgumentsAvailable++;
                   break;
               case 'o':
                   String host = g.getOptarg();
                   if (!host.endsWith("/")) host+="/";
                   commandLineArgs.put("host",host);
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'd':
                   String dir = g.getOptarg();
                   if (dir.endsWith(File.separator))
                       dir = dir.substring(0, dir.length()-1);
                   commandLineArgs.put("buildDir",dir);
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'a':
                   String adir = g.getOptarg();
                   if (adir.endsWith(File.separator))
                       adir = adir.substring(0, adir.length()-1);
                   commandLineArgs.put("archiveDir",adir);
                   noOfRequiredArgumentsAvailable++;
                   break;
               case 'm':
                   commandLineArgs.put("mpragesessionid",g.getOptarg());
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
    
       
    public PETSessionDocument getPETSessionFromHost() throws Exception {
         String createdFile = new XMLSearch(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).searchFirst("xnat:petSessionData.ID",commandLineArgs.get("xnatId"), "=","xnat:petSessionData",FileUtils.getTempFolder());
         //Bind the instance to the generated XMLBeans types.
         PETSessionDocument petSession = (PETSessionDocument)new XmlReader().read(createdFile, true);
         boolean store = false;
         String archiveDir = commandLineArgs.get("archiveDir");
         if (petSession.getPETSession().isSetReconstructions()) {
             logger.info("PreExisting Reconstruction exists. Deleting this reconstruction element");
             petSession.getPETSession().unsetReconstructions();
             if (petSession.getPETSession().isSetRegions()) {
                 logger.info("Preexisting regions exist. Deleting them");
                 File roisDir = new File(archiveDir + File.separator + commandLineArgs.get("sessionId") + File.separator + "ROIS");
                 if (roisDir.exists()) {
                     deleteDirectory(roisDir);
                 }
                petSession.getPETSession().unsetRegions();
             }
             store = true;
         }
         if (petSession.getPETSession().isSetAssessors()) {
             logger.info("PreExisting Assessors exists. Deleting this assessor element");
             petSession.getPETSession().unsetAssessors();
             File qcDir = new File(archiveDir + File.separator + commandLineArgs.get("sessionId") + File.separator + "QC");
             if (qcDir.exists()) {
                 deleteDirectory(qcDir); 
             }
             File processedDir = new File(archiveDir + File.separator + commandLineArgs.get("sessionId") + File.separator + "PROCESSED");
             if (processedDir.exists()) {
                 deleteDirectory(processedDir); 
             }
             store = true;
         }
         
         if (store) {
             new XMLStore(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).store(petSession.xmlText(new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces()));
             logger.info("Session stored");
         }
         return petSession;
    }
    
    private boolean deleteDirectory(File path) {
        if( path.exists() ) {
          File[] files = path.listFiles();
          for(int i=0; i<files.length; i++) {
             if(files[i].isDirectory()) {
               deleteDirectory(files[i]);
             }
             else {
               files[i].delete();
             }
          }
        }
        return( path.delete() );
      }

   
 
    
    private void setXNATUserNamePassword() throws UserNameNotFoundException {
        if (commandLineArgs.get("username")==null || commandLineArgs.get("password") == null) {
            XNATPassFileParser xnatPassFileParser = new XNATPassFileParser();
            if (xnatPassFileParser.passFileExists()) {
                String xnatUserName = xnatPassFileParser.getUserName(commandLineArgs.get("host"));
                String password = xnatPassFileParser.getUsersPassword(xnatUserName,commandLineArgs.get("host"));
                commandLineArgs.put("username",xnatUserName);
                commandLineArgs.put("password",password);
            }
        }
    }
    
    
    private void setAssociatedMRSessionField(PETSessionDocument petSession) {
        Fields fields = null;
        if (petSession.getPETSession().isSetFields()) {
            fields = petSession.getPETSession().getFields();
        }else {
            fields = petSession.getPETSession().addNewFields();
        }
        
        Field mrSessionField = null;
        if (fields.getFieldArray() != null) {
            for (int i =0; i < fields.getFieldArray().length; i++) {
                if (fields.getFieldArray(i).getName().equals("MRSESSION")) {
                    mrSessionField = fields.getFieldArray(i);
                    break;
                }
            }
        }
        if (mrSessionField == null) {
            mrSessionField = fields.addNewField();
        }
        mrSessionField.setName("MRSESSION");
        mrSessionField.setStringValue(commandLineArgs.get("mpragesessionid"));
    }
    
    public void createReconstructedImage() throws Exception {
        try {
            setXNATUserNamePassword();
        }catch(UserNameNotFoundException ue) {
            throw new Exception("Couldnt proceed due to lack of username and password");
        }
        PETSessionDocument petSession = getPETSessionFromHost();
        
        setAssociatedMRSessionField(petSession);
        String pathToPipelineFile = commandLineArgs.get("pipelineXml");
        File xmlFile = new File(pathToPipelineFile);
        if (!xmlFile.exists()) {
            if (commandLineArgs.containsKey("buildDir") && commandLineArgs.containsKey("archiveDir") && commandLineArgs.get("pipelineXml").startsWith(commandLineArgs.get("buildDir"))) {
                pathToPipelineFile = StringUtils.replace(pathToPipelineFile,commandLineArgs.get("buildDir"), commandLineArgs.get("archiveDir"));
            }
        }
        System.out.println("Reading " + pathToPipelineFile);
        XmlObject xmlObject = new XmlReader().read(pathToPipelineFile, false);
        if (!(xmlObject instanceof AllResolvedStepsDocument)) {
            throw new PipelineException("Invalid XML file supplied " + commandLineArgs.get("pipelineXml") + " ==> Expecting a resolved pipeline document");
        }
        AllResolvedStepsDocument pipelineDoc = (AllResolvedStepsDocument)xmlObject;
        String errors = XMLBeansUtils.validateAndGetErrors(pipelineDoc);
        if (errors != null) {
            throw new XmlException("Invalid XML " + commandLineArgs.get("pipelineXml") + "\n" + errors);
        }
        /*for (int i = 0; i < pipelineDoc.getAllResolvedSteps().sizeOfResolvedStepArray(); i++) {
            System.out.println("Steps are " + pipelineDoc.getAllResolvedSteps().getResolvedStepArray(i).getId());
        }*/
        
        ReconstructedImageData recon = null;
        
        if (petSession.getPETSession().isSetReconstructions()) {
        	Reconstructions recons = petSession.getPETSession().getReconstructions();
        	recon = recons.addNewReconstructedImage();
        }else {
            recon = petSession.getPETSession().addNewReconstructions().addNewReconstructedImage();
        }

	    DateFormat dateFormat = new SimpleDateFormat("yyMMdd");
	    Date now = Calendar.getInstance().getTime();
        
        String id =petSession.getPETSession().getID() + "_" + commandLineArgs.get("type") + "_" +dateFormat.format(now) ;
        
        recon.setID(id);
        recon.setType(commandLineArgs.get("type"));
        recon.setImageSessionID(petSession.getPETSession().getID());
        //recon.setBaseScanType(commandLineArgs.get("baseScanType"));
        if (stepIds == null || stepIds.size() == 0) {
            stepIndices = XMLBeansUtils.getAllStepIds(pipelineDoc);
            Enumeration<String> keys = stepIndices.keys();
            while (keys.hasMoreElements())
                stepIds.add(keys.nextElement());
        }
        addScanIds(pipelineDoc,recon, petSession);
        addOutputFiles(pipelineDoc,recon, petSession);
        setProvenance(pipelineDoc,recon,petSession);
        addComputations(recon,petSession);
        try {
            petSession.save(new File(commandLineArgs.get("sessionId")+".xml"),new XmlOptions().setSavePrettyPrint());
        }catch(Exception e){} 
        new XMLStore(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).store(petSession.xmlText(new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces()));
        logger.info("Session stored");
    }
    
    private void addComputations(ReconstructedImageData recon, PETSessionDocument petSession) {
        //Parse the computation file and insert computations
        if (commandLineArgs.containsKey("computation")) { 
            try {
                LabeledCSVParser lcsvp = new LabeledCSVParser(new CSVParser(new BufferedReader( new FileReader(commandLineArgs.get("computation")) )));
                String[] labels = lcsvp.getLabels();
                Computations computations = null;
                if (recon.isSetComputations()) {
                    computations = recon.getComputations();
                }
                while (lcsvp.getLine() != null) {
                    String name = lcsvp.getValueByLabel("NAME");
                    String value = lcsvp.getValueByLabel("VALUE");
                    String source = lcsvp.getValueByLabel("SOURCE");
                    String units = lcsvp.getValueByLabel("UNITS");
                    if (name !=null && value != null ) {
                        if (computations == null) {computations = recon.addNewComputations();}
                        ComputationData datum = computations.addNewDatum();
                        datum.setName(name);datum.setValue(value);
                        if (source != null && !source.equals("")) {
                            datum.setSource(source);
                        }else datum.setSource("NA");
                        if (units != null && !units.equals("")) {
                            datum.setUnits(units);
                        }
                    }
                }
            }catch(IOException fnfe) {System.out.println("Couldnt find compuation file");}
        }
    }
    
    
   
    
    
    
    private void addOutputFiles(AllResolvedStepsDocument pipelineDoc,ReconstructedImageData recon, PETSessionDocument petSession) throws PipelineException {
        String relativePath = FileUtils.getRelativePath(petSession);
        String sessionId = commandLineArgs.get("sessionId");

        for (String stepId : stepIds) {
            System.out.println("Step ID " + stepId);
            ArrayList <Integer> indices = new ArrayList<Integer>();
            if (stepIndices.containsKey(stepId)) {
                indices = stepIndices.get(stepId);
            }else {
                indices = XMLBeansUtils.getStepIndicesById(pipelineDoc,stepId);
                stepIndices.put(stepId,indices);
            }
            System.out.println("Indices are " + indices);
            if (indices != null && indices.size() > 0) {
                for (int i = 0; i < indices.size(); i++) {
                    ResolvedStep rStep = pipelineDoc.getAllResolvedSteps().getResolvedStepArray((Integer)indices.get(i).intValue());
                    Out reconOut = null;
                    if (recon.isSetOut()) {
                        reconOut = recon.getOut();
                    }
                    if (rStep.sizeOfResolvedOutputArray() > 0) {
                        for (ResolvedOutput output : rStep.getResolvedOutputArray()) {
                            if (output.isSetFile() && output.getFile().isSetName()) {
                                XmlObject absRsc = null;
                                OutputData.File file = output.getFile();
                                String xsiType = file.getXsiType();
                                //if (commandLineArgs.containsKey("buildDir") && commandLineArgs.containsKey("archiveDir"))
                                //    file.getPath().setStringValue(StringUtils.replace(file.getPath().getStringValue(),(String)commandLineArgs.get("buildDir"),(String)commandLineArgs.get("archiveDir")));
                                if (!FileUtils.exists(file.getPath().getStringValue() + File.separator + file.getName())) {
                                    System.out.println("Couldnt find file " + file.getPath().getStringValue() + File.separator + file.getName());
                                    continue;
                                }
                                if (!file.isSetFormat()) {
                                    System.out.println("Missing format for file " + file.getPath().getStringValue() + File.separator + file.getName());
                                    continue;
                                }
                                if (xsiType.equals("xnat:imageResource")) {
                                    PlexiImageHeaderReader pihr = null;
                                    FileInfo fi  = null;
                                    try {
                                        pihr = new PlexiImageHeaderReader(file.getFormat());
                                       fi = pihr.getFileInfo(file.getPath().getStringValue(), file.getName());
                                    }catch(IOException ioe) {
                                        throw new PipelineException("Couldnt find file file:///" + file.getPath().getStringValue() + "/" + file.getName());
                                    }
                                    if (fi != null) {
                                        if (reconOut == null)
                                            reconOut = recon.addNewOut();
                                        absRsc = reconOut.addNewFile();
                                        ImageResource rsc = (ImageResource)absRsc.changeType(ImageResource.type);
                                        rsc.setURI(FileUtils.getPath(file.getPath().getStringValue(),relativePath, sessionId) + File.separator + file.getName());
                                        rsc.setFormat(file.getFormat());
                                        if (file.isSetContent()) rsc.setContent(file.getContent());
                                        if (file.isSetDescription()) rsc.setDescription(file.getDescription());
                                        Dimensions dim = ImageResource.Dimensions.Factory.newInstance();
                                        dim.setX(new java.math.BigInteger(Integer.toString(fi.width))); 
                                        dim.setY(new java.math.BigInteger(Integer.toString(fi.height)));
                                        dim.setZ(new java.math.BigInteger(Integer.toString(fi.nImages)));
                                        dim.setVolumes(new java.math.BigInteger(Integer.toString(pihr.getVolumes())));
                                        rsc.setDimensions(dim);
                                        VoxelRes voxelRes = ImageResource.VoxelRes.Factory.newInstance();
                                        voxelRes.setX((float)fi.pixelWidth);
                                        voxelRes.setY((float)fi.pixelHeight);
                                        voxelRes.setZ((float)fi.pixelDepth);
                                        rsc.setVoxelRes(voxelRes);
                                        rsc.setOrientation(pihr.getOrientation());
                                    }else {throw new PipelineException("Couldnt get image file parameters for file " + file.getPath().getStringValue() + File.separator + file.getName());}
                                }else  if (xsiType.equals("xnat:resource")) {
                                    if (reconOut == null)
                                        reconOut = recon.addNewOut();
                                    absRsc = reconOut.addNewFile();
                                    Resource rsc = (Resource)absRsc.changeType(Resource.type);
                                    rsc.setURI(FileUtils.getPath(file.getPath().getStringValue(),relativePath, sessionId) + File.separator + file.getName()) ;
                                    rsc.setFormat(file.getFormat());
                                    if (file.isSetContent()) rsc.setContent(file.getContent());
                                    if (file.isSetDescription()) rsc.setDescription(file.getDescription());
                                }
                            }
                        }
                    }
                }
            }else throw new PipelineException("Couldnt find any Resolved Steps for the Step Ids");
        }        
    }
    
    private void setProvenance(AllResolvedStepsDocument pipelineDoc,ReconstructedImageData recon, PETSessionDocument petSession) {
        for (String stepId : stepIds) {
            ArrayList indices = null;
            if (stepIndices.get(stepId) != null) {
                indices = stepIndices.get(stepId);
            }else 
                indices = XMLBeansUtils.getStepIndicesById(pipelineDoc,stepId);
            if (indices != null && indices.size() > 0) {
                net.nbirn.prov.Process reconProv = recon.getProvenance();
                if (!recon.isSetProvenance())
                    reconProv = recon.addNewProvenance();
                for (int i = 0; i < indices.size(); i++) {
                    Provenance prov = pipelineDoc.getAllResolvedSteps().getResolvedStepArray(((Integer)indices.get(i)).intValue()).getProvenance();
                    if (prov!=null) {
                        for (int j = 0; j < prov.sizeOfProcessStepArray(); j++) {
                            reconProv.addNewProcessStep();
                            reconProv.setProcessStepArray(reconProv.sizeOfProcessStepArray()-1, prov.getProcessStepArray(j));
                        }
                    }
                }
            }
        }
        
    }
    
    private void addScanIds(AllResolvedStepsDocument pipelineDoc,ReconstructedImageData recon, PETSessionDocument petSession) {
        ArrayList<String> scanIds = new ArrayList<String>();
        if (inScanIds != null && inScanIds.size() > 0) {
            for (String scanId : inScanIds) {
                scanIds.add(scanId);
            }
        }else if (commandLineArgs.containsKey("scanParameterName")) {
          ParameterData param =  XMLBeansUtils.getParameterByName(pipelineDoc,commandLineArgs.get("scanParameterName"));
          if (param.getValues().isSetUnique()) {
              scanIds.add(param.getValues().getUnique());
          }else {
              for (String scanId : param.getValues().getListArray() ) {
                  scanIds.add(scanId);
              }
          }
        }
        if (validScanIds(scanIds, petSession)) {
            InScans inScans = recon.addNewInScans();
            for (String scanId : scanIds) {
                inScans.addScanID(scanId);
                //addScanFile(recon,scanId,mrSession);
            }
        }
    }
    
    private boolean validScanIds(ArrayList<String> scanIds, PETSessionDocument petSession) {
        boolean rtn = false;
        for (String scanId : scanIds) {
            rtn = false;
            for (ImageScanData scan : petSession.getPETSession().getScans().getScanArray()) {
                if (scan.getID().equals(scanId)) {
                    rtn = true; break;
                }
            }
        }
        return rtn;
    }
    
    private void addScanFile(ReconstructedImageData recon, String scanId, PETSessionDocument petSession) {
        PetScanData scanById = null;
        for (ImageScanData scan : petSession.getPETSession().getScans().getScanArray()) {
            if (scan.getID().equals(scanId)) {
                scanById = (PetScanData)scan; break;
            }
        }
        if (scanById == null) return;
        if (scanById.sizeOfFileArray() > 0) {
            In inFiles = null;
            if (recon.isSetIn())
                inFiles = recon.getIn();
            if (inFiles == null)
             inFiles = recon.addNewIn();
            if (scanById.sizeOfFileArray() > 0) {
                for (int i = 0 ; i < scanById.sizeOfFileArray(); i++) {
                    inFiles.addNewFile();
                    inFiles.setFileArray(inFiles.sizeOfFileArray()-1,scanById.getFileArray(i));
                }
            }
        }
    }
    
     
    public void printUsage() {
        String usage = "PETReconstructedImageCreator  \n";
        usage += "Options:\n";
        usage += "\t -sessionId <session label>\n";
        usage += "\t -xnatId <session XNAT id>\n";
        usage += "\t -type: <type for Reconstructed Image> \n";
        usage += "\t -baseScanType: <baseScanType for Reconstructed Image> \n";
        usage += "\t -scan: <In Scan Ids for the Reconstructed Image>\n";
        usage += "\t -scanParameterName: <Name of the parameter in the pipeline xml whose values are to be used for in scan ids>\n";
        usage += "\t -stepId: <Step id from which output files and provennace is to be added>\n";
        usage += "\t\t use more than once for more steps\n";
        usage += "\t -pipelineXml: <Path to pipeline xml file>\n";
        usage += "\t -u: XNAT username [Optional: will parse .xnatPass file] \n";
        usage += "\t -pwd: XNAT password [Optional: will parse .xnatPass file]\n";
        usage += "\t -host: URL to XNAT based Website\n";
        usage += "\t -buildDir: the path to directory where the session was built\n";
        usage += "\t -archiveDir: the parent path to session folder in the archive\n";
        usage += "\t -log <path to log4j.properties file>\n";
        usage += "\t -computation <path to csv computation file>\n";
        usage += "\t -help\n";

        System.out.println(usage);
        System.exit(1);
    }
    
    public String getLogPropertiesFile() {
        return (String)commandLineArgs.get("log");
    }

    
    public static void main(String args[]) {
        try {
            PETReconstructedImageCreator recon = new PETReconstructedImageCreator(args);
            if (recon.getLogPropertiesFile() != null) {
                PropertyConfigurator.configure(recon.getLogPropertiesFile());
            }else {
                BasicConfigurator.configure();
            }
            recon.createReconstructedImage();
            System.exit(0);
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    static Logger logger = Logger.getLogger(PETReconstructedImageCreator.class);
    
    Hashtable<String,ArrayList<Integer>> stepIndices; 
    Hashtable<String,String> commandLineArgs;
    ArrayList<String> inScanIds;
    ArrayList<String> stepIds;
    int noOfRequiredArguments = 9;
    int noOfRequiredArgumentsAvailable = 0;
    String service_session;
    Service service;

}
