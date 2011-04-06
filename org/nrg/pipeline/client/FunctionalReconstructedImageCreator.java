/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlOptions;
import org.nrg.pipeline.exception.PipelineException;
import org.nrg.pipeline.utils.XMLBeansUtils;
import org.nrg.pipeline.xmlbeans.AllResolvedStepsDocument;
import org.nrg.pipeline.xmlbeans.xnat.ComputationData;
import org.nrg.pipeline.xmlbeans.xnat.MRSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageDocument;
import org.nrg.pipeline.xmlbeans.xnat.ReconstructedImageData.Computations;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnattools.xml.XMLStore;

public class FunctionalReconstructedImageCreator extends ReconstructedImageCreator{
    
    public FunctionalReconstructedImageCreator(String argv[]) {
        super(argv);
    }
    
    public void createReconstructedImage() throws Exception {
        /*try {
            setXNATUserNamePassword();
        }catch(UserNameNotFoundException ue) {
            throw new Exception("Couldnt proceed due to lack of username and password");
        }*/
        MRSessionDocument mrSession = getMrSessionFromHost();
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

        ReconstructedImageDocument reconDoc = ReconstructedImageDocument.Factory.newInstance();

        ReconstructedImageData recon = reconDoc.addNewReconstructedImage();

	    DateFormat dateFormat = new SimpleDateFormat("yyMMdd");
	    DateFormat xmldateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    Date now = Calendar.getInstance().getTime();
        String sessionId = commandLineArgs.get("xnatId");
        
        String existingReconId =sessionId + "_" + commandLineArgs.get("type") + "_" +dateFormat.format(now) ;
        recon.setID(existingReconId);
        recon.setImageSessionID(sessionId);
        recon.setType(commandLineArgs.get("type"));
        recon.setBaseScanType(commandLineArgs.get("baseScanType"));
        if (stepIds == null || stepIds.size() == 0) {
            stepIndices = XMLBeansUtils.getAllStepIds(pipelineDoc);
            Enumeration<String> keys = stepIndices.keys();
            while (keys.hasMoreElements())
                stepIds.add(keys.nextElement());
        }
        addScanIds(pipelineDoc,recon, mrSession);
        addOutputFiles(pipelineDoc,recon, mrSession);
        setProvenance(pipelineDoc,recon,mrSession);
        
        addQCData(recon);
        
        try {
            new XMLStore(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).store(reconDoc.xmlText(new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces()));
            System.out.println("Session stored");
        }catch(Exception e){
            recon.save(new File(commandLineArgs.get("sessionId")),new XmlOptions().setSavePrettyPrint());
            e.printStackTrace();
        } 
        System.out.println("Session stored");
    }
    
    private void addQCData(ReconstructedImageData recon) {
        String qcdir = commandLineArgs.get("buildDir") + File.separator + commandLineArgs.get("sessionId") + File.separator + "QC";
  //      String qcAdir =  commandLineArgs.get("sessionId") + File.separator + "QC";
       	//Changed by MR on June 14, 2010 to contain the ArchiveDir absolute path in the Session documents
    	String qcAdir = commandLineArgs.get("archiveDir");
    	if (!qcAdir.endsWith("/")) qcAdir += "/";
    	qcAdir =  commandLineArgs.get("sessionId") + File.separator + "QC";
        String sessionId = commandLineArgs.get("sessionId");
 
        File dir = new File(qcdir);
        if (dir.exists()) {
            String[] children = dir.list();
            if (children != null) {
                Computations computations = recon.addNewComputations();
                for (int i=0; i<children.length; i++) {
                    String filename = children[i];
                    if (filename.endsWith(".dat")) {
                        Properties properties = new Properties();
                        try {
                            properties.load(new FileInputStream(qcdir + File.separator + filename));
                            Enumeration keys = properties.keys();
                            while (keys.hasMoreElements()) {
                                String key = (String)keys.nextElement();
                                if (key.startsWith("SPATIAL.CORRELATION")) {
                                    ComputationData datum = computations.addNewDatum();
                                    datum.setName(key);
                                    datum.setSource(qcAdir + File.separator + filename);
                                    datum.setValue(properties.getProperty(key));
                                }
                            }
                            
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            
        }

        
    }
    
    public static void main(String args[]) {
        try {
            FunctionalReconstructedImageCreator recon = new FunctionalReconstructedImageCreator(args);
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
    
}
