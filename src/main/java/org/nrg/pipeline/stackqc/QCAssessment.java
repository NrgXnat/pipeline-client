/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.stackqc;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.xmlbeans.XmlCalendar;
import org.apache.xmlbeans.XmlCursor;
import org.apache.xmlbeans.XmlOptions;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.xmlbeans.xnat.MRSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.QCAssessmentDocument;
import org.nrg.pipeline.xmlbeans.xnat.QcAssessmentData;
import org.nrg.pipeline.xmlbeans.xnat.StatisticsData;
import org.nrg.pipeline.xmlbeans.xnat.QcAssessmentData.Scans;
import org.nrg.pipeline.xmlbeans.xnat.QcAssessmentData.Scans.Scan;
import org.nrg.pipeline.xmlbeans.xnat.QcAssessmentData.Scans.Scan.SliceQC;
import org.nrg.pipeline.xmlbeans.xnat.QcAssessmentData.Scans.Scan.SliceQC.Slice;
import org.nrg.pipeline.xmlbeans.xnat.StatisticsData.AddField;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnattools.xml.MRXMLSearch;
import org.nrg.xnattools.xml.XMLSearch;
import org.nrg.xnattools.xml.XMLStore;

/*
 * This class creates QCAssessment entries after running StackCheck_d4fp
 * Expects as input a tabbed file containing scan id and stack check report
 * Parses the report to create the scanstatistics element. 
 */
public class QCAssessment {
    
    String sessionId;
    String qcData;
    String host;
    String user;
    String password;
    String project;
    String relativePath;
    
    public QCAssessment(String argv[]) {
        int c;
        int argsAvailable = 0;

        LongOpt[] longopts = new LongOpt[6];
        longopts[0] = new LongOpt("sessionId", LongOpt.REQUIRED_ARGUMENT, null, 's'); 
        longopts[1] = new LongOpt("qcData", LongOpt.REQUIRED_ARGUMENT, null, 'q');
        longopts[2] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'h');
        longopts[3] = new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'u');
        longopts[4] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'p');
        longopts[5] = new LongOpt("project", LongOpt.REQUIRED_ARGUMENT, null, 'r'); 
        
        Getopt g = new Getopt("QCAssessment", argv, "s:q:h:u:r:p:;", longopts, true);
        g.setOpterr(false); // We'll do our own error handling
        //
        while ((c = g.getopt()) != -1) {
          switch (c)
            {
                case 's':
                    sessionId = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'q':
                    qcData = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'h':
                    host = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'u':
                    user = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'p':
                    password = g.getOptarg();
                    argsAvailable++;
                    break;
                case 'r':
                    project = g.getOptarg();
                    argsAvailable++;
                    break;
            }
        }
        
        if (argsAvailable != 6) {
            printUsage();
            System.exit(1);
        }
    }
    
    /*
     * Parse the qcData file to extract the path to report file for each 
     * scan
     */
    public void createAssessment() {
        QCAssessmentDocument qcAssessmentDoc = QCAssessmentDocument.Factory.newInstance();
        //declared here only to make visible to finally clause
        BufferedReader input = null;
        try {
            MRSessionDocument mrSession = getMrSessionFromHost(); 
        	relativePath = FileUtils.getRelativePath(mrSession);

            QcAssessmentData qcAssessmentData = qcAssessmentDoc.getQCAssessment();
            if (qcAssessmentData == null) {
                qcAssessmentData = qcAssessmentDoc.addNewQCAssessment();
            }
            Calendar calendar = Calendar.getInstance();
            Date date = calendar.getTime();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String idSuffix = dateFormat.format(date);
            XmlCalendar xmlCalendar = new XmlCalendar(idSuffix);
            
            DateFormat dateFormat1 = new SimpleDateFormat("yyMMdd");
    	    Date now = Calendar.getInstance().getTime();
    	    String now_formatted = dateFormat.format(now);
            
            qcAssessmentData.setID(sessionId+"_stkchkQC_"+now_formatted);
            qcAssessmentData.setLabel(sessionId+"_stkchkQC_"+now_formatted);
            
            //TODO Change this to Project when the project comes through
            //qcAssessmentData.setProject(mrSession.getMRSession().getSessionType());
            qcAssessmentData.setDate(xmlCalendar);
            qcAssessmentData.setImageSessionID(sessionId);
            qcAssessmentData.setProject(project);

            //use buffering, reading one line at a time
          //FileReader always assumes default encoding is OK!
          input = new BufferedReader( new FileReader(qcData) );
          String line = null; //not declared within while loop
          /*
          * readLine is a bit quirky :
          * it returns the content of a line MINUS the newline.
          * it returns null only for the END of the stream.
          * it returns an empty String if two newlines appear in a row.
          */
          while (( line = input.readLine()) != null){
            String tokens[] = line.split(" ");
            if (tokens == null || tokens.length != 2) {
                System.out.println("Invalid/Insufficient content in " + qcData);
                System.exit(1);
            }
            String scanId = tokens[0];
            String reportFilePath = tokens[1];
            addScanQC(qcAssessmentData,scanId,reportFilePath);
          }
         // qcAssessmentDoc.save(new File("qc.xml"),new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces());
          if (qcAssessmentData.getScans().sizeOfScanArray() > 0) {
             // qcAssessmentDoc.save(new File("qc.xml"),new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces());
        	  new XMLStore(host, user, password).store(qcAssessmentDoc.xmlText(new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces()));
          }
        }
        catch (FileNotFoundException ex) {
          ex.printStackTrace();
          System.exit(1);
        }
        catch (IOException ex){
          ex.printStackTrace();
          System.exit(1);
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);  
        }
        finally {
          try {
            if (input!= null) {
              //flush and close both "input" and its underlying FileReader
              input.close();
            }
          }
          catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
          }
        }
    }
    
    /**
     * @param qcAssessmentData
     * @param scanId
     * @param reportFilePath
     */
    private void  addScanQC(QcAssessmentData qcAssessmentData,String scanId,String reportFilePath) {
        BufferedReader input = null;
        try {
            //use buffering, reading one line at a time
          //FileReader always assumes default encoding is OK!
          input = new BufferedReader( new FileReader(reportFilePath) );
          String line = null; //not declared within while loop
          /*
          * readLine is a bit quirky :
          * it returns the content of a line MINUS the newline.
          * it returns null only for the END of the stream.
          * it returns an empty String if two newlines appear in a row.
          */
          Scans scans = qcAssessmentData.getScans();
          if (scans == null)
             scans = qcAssessmentData.addNewScans();
          boolean skip = true;
          Scan aScan = scans.addNewScan();
          aScan.setId(scanId);
          while (( line = input.readLine()) != null){
              if (skip && !line.startsWith("Threshold")) continue;
              else if (line.startsWith("Threshold")) {
                  skip = false;
                  String parts[] = line.split(":");
                  StatisticsData scanStats = (StatisticsData)aScan.addNewScanStatistics().changeType(StatisticsData.type);
                  AddField addField = scanStats.addNewAddField();
                  addField.setName("Threshold");
                  addField.setStringValue(parts[1]);
                  addField = scanStats.addNewAddField();
                  addField.setName("Report file");
                  addField.setStringValue(FileUtils.getPath(reportFilePath,relativePath, sessionId));
              }else if (line.startsWith("slice") || line.startsWith(" ")) {
                  continue;
              } else if (line.startsWith("VOXEL")) {
                 /* String parts[] = line.split(" ");
                  AbstractStatistics absStats = aScan.getScanStatistics();
                  if (absStats == null) {
                      absStats = aScan.addNewScanStatistics();
                  }
                  StatisticsData statsData = (StatisticsData)absStats.changeType(StatisticsData.type);
                  statsData.setNoOfVoxels(new BigInteger(parts[1]));
                  statsData.setMean(Double.parseDouble(parts[2]));
                  statsData.setStddev(Double.parseDouble(parts[3]));
                  statsData.setSnr(Double.parseDouble(parts[4]));
                  statsData.setMin(Double.parseDouble(parts[5]));
                  statsData.setMax(Double.parseDouble(parts[6]));*/
                  continue;
              }else {
                  String parts[] = line.split(" ");
                  if (parts == null || parts.length != 7) {
                      parts = line.split("\t");
                  }
                  if (parts == null || parts.length <7) {
                      continue;
                  }
                  SliceQC sliceQc = aScan.getSliceQC();
                  if (sliceQc == null) sliceQc = aScan.addNewSliceQC();
                  Slice slice = sliceQc.addNewSlice();
                  slice.setNumber(parts[0]);
                  StatisticsData stats = (StatisticsData)slice.addNewSliceStatistics().changeType(StatisticsData.type);
                  
                  XmlCursor statsCursor = stats.newCursor();
                  statsCursor.toFirstContentToken();
                  String namespaceUri = "http://nrg.wustl.edu/xnat";

                  for (int i=1; i< 7; i++) {
                	  if (parts[i].trim().equals("Inf")) {
                		  parts[i] = parts[i].toUpperCase();
                	  }
                  }
                  

                  statsCursor.beginElement("mean",namespaceUri);
                  statsCursor.insertChars(parts[2]);
                  statsCursor.toNextToken();

                  statsCursor.beginElement("snr",namespaceUri);
                  statsCursor.insertChars(parts[4]);
                  statsCursor.toNextToken();

                  statsCursor.beginElement("min",namespaceUri);
                  statsCursor.insertChars(parts[5]);
                  statsCursor.toNextToken();

                  statsCursor.beginElement("max",namespaceUri);
                  statsCursor.insertChars(parts[6]);
                  statsCursor.toNextToken();

                  
                  statsCursor.beginElement("stddev",namespaceUri);
                  statsCursor.insertChars(parts[3]);
                  statsCursor.toNextToken();

                  statsCursor.beginElement("no_of_voxels",namespaceUri);
                  statsCursor.insertChars(parts[1]);
                  statsCursor.toNextToken();

                  statsCursor.beginElement("additionalStatistics",namespaceUri);
                  statsCursor.insertAttributeWithValue("name","No of Out");
                  statsCursor.insertChars(parts[7]);
                  
                  
                  statsCursor.dispose(); 
                  
                  /*stats.setNoOfVoxels(new BigInteger(parts[1]));
                  XmlDouble dbl = org.apache.xmlbeans.XmlDouble.Factory.newInstance();
                  Double d = Double.parseDouble(parts[2]);
              	  dbl.setDoubleValue(d.doubleValue());
                  stats.setMean(dbl.getDoubleValue());
                  d = Double.parseDouble(parts[3]);
              	  dbl.setDoubleValue(d.doubleValue());
                  stats.setStddev(dbl.getDoubleValue());
                  d = Double.parseDouble(parts[4]);
              	  dbl.setDoubleValue(d.doubleValue());
              	  stats.setSnr(dbl.getDoubleValue());
                  d = Double.parseDouble(parts[5]);
              	  dbl.setDoubleValue(d.doubleValue());
                  stats.setMin(dbl.getDoubleValue());
                  d = Double.parseDouble(parts[6]);
              	  dbl.setDoubleValue(d.doubleValue());
                  stats.setMax(dbl.getDoubleValue());*/ 
              }
          }
        }
        catch (FileNotFoundException ex) {
          ex.printStackTrace();
          System.exit(1);
        }
        catch (IOException ex){
          ex.printStackTrace();
          System.exit(1);
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);  
        }
        finally {
          try {
            if (input!= null) {
              //flush and close both "input" and its underlying FileReader
              input.close();
            }
          }
          catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
          }
        }
        
    }
    
    protected MRSessionDocument getMrSessionFromHost() throws Exception {
    	MRSessionDocument mrSession = new MRXMLSearch(host, user, password).getMrSessionFromHost(sessionId);
        return mrSession;
   }
    
    public void printUsage() {
        System.out.println("QCAssessment -sessionId <Session Id> -qcData <file containing scan and report file path> -host <Xnat host> -u <Xnat user> -pwd <Xnat pwd>");
    }
    
    public static void main(String args[]) {
        QCAssessment qcAssessment = new QCAssessment(args);
       qcAssessment.createAssessment();
       System.out.println("Created QC Element");
    }
}
