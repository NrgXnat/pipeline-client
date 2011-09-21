/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.unix.utils;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.exception.PipelineException;
import org.nrg.pipeline.process.LocalProcessLauncher;
import org.nrg.pipeline.utils.CommandStatementPresenter;
import org.nrg.pipeline.utils.XMLBeansUtils;
import org.nrg.pipeline.xmlbeans.ParametersDocument;
import org.nrg.pipeline.xmlbeans.xnat.MRSessionDocument;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.pipeline.xpath.XPathResolverSaxon;
import org.nrg.xnattools.xml.MRXMLSearch;

public class LnList {
	
	/*
	 * This class will launch ln -s <from directory> <to directory>
	 * for a list of sessions. This class has been created for FREESURFER where SUBJECTS_DIR needs to be created. 
	 */
	
	
	public LnList(String[] argv) {
			int c;
			commandLineArgs = new Hashtable<String,String>();

		   LongOpt[] longopts = new LongOpt[9];
	        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
	        longopts[1] = new LongOpt("parameter-file", LongOpt.REQUIRED_ARGUMENT, null, 'p'); 
	        longopts[2] = new LongOpt("parameter-name", LongOpt.REQUIRED_ARGUMENT, null, 'n');
	        longopts[3] = new LongOpt("working-dir", LongOpt.REQUIRED_ARGUMENT, null, 'd');
	        longopts[4] = new LongOpt("from-subdir", LongOpt.REQUIRED_ARGUMENT, null, 'f');
	       // longopts[5] = new LongOpt("to-subdir", LongOpt.REQUIRED_ARGUMENT, null, 't');
	        longopts[5] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o');
	        longopts[6] = new LongOpt("user", LongOpt.REQUIRED_ARGUMENT, null, 'u');
	        longopts[7] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'w');
	        longopts[8] = new LongOpt("log", LongOpt.REQUIRED_ARGUMENT, null, 'l');
	        Getopt g = new Getopt("LsList", argv, "p:n:w:f:o:u:w:l:h;", longopts, true);
	        g.setOpterr(false); // We'll do our own error handling
	        //
	        while ((c = g.getopt()) != -1) {
	          switch (c) {
	                case 'p':
	                    commandLineArgs.put("parameter-file",g.getOptarg());
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                case 'n':
	                    commandLineArgs.put("parameter-name",g.getOptarg());
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                case 'd':
	                    commandLineArgs.put("working-dir",g.getOptarg());
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                case 'f':
	                    commandLineArgs.put("from-subdir",g.getOptarg());
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                //case 't':
	                 //   commandLineArgs.put("to-subdir",g.getOptarg());
	                 //   noOfRequiredArgumentsAvailable++;
	                 //   break;
	                case 'o':
	                    String host = g.getOptarg();
	                    if (!host.endsWith("/")) host+="/";
	                    commandLineArgs.put("host",host);
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                case 'w':
	                    commandLineArgs.put("pwd",g.getOptarg());
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                case 'u':
	                    commandLineArgs.put("user",g.getOptarg());
	                    noOfRequiredArgumentsAvailable++;
	                    break;
	                case 'l':
	                    commandLineArgs.put("log",g.getOptarg());
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
	
	public void launch() throws Exception {
		ArrayList sessionIds = getValuesForParameter();
		if (sessionIds == null) throw new PipelineException("No values found for parameter " + commandLineArgs.get("parameter-name"));
		String workingFolder = commandLineArgs.get("working-dir") ;
		if (!workingFolder.endsWith(File.separator)) workingFolder += File.separator;
		for (int i = 0; i < sessionIds.size(); i++) { 
			try { 
				String sessionPath = null; String sessionName = null;
				try {
					MRSessionDocument mrSession= new MRXMLSearch(commandLineArgs.get("host"), commandLineArgs.get("user"), commandLineArgs.get("pwd")).getMrSessionFromHost(sessionIds.get(i).toString(),  commandLineArgs.get("working-dir"));
					 sessionPath = getArchivePathForSession(mrSession);
					 if (!sessionPath.endsWith(File.separator)) sessionPath += File.separator;
					 sessionPath += mrSession.getMRSession().getID();
					 if (commandLineArgs.get("from-subdir") != null) {
						 sessionPath += File.separator + commandLineArgs.get("from-subdir");
					 }
					 sessionName = workingFolder + mrSession.getMRSession().getID();
				}catch(Exception e) {
					 logger.debug(e);
					 System.out.println("Session " + sessionIds.get(i) + " Encountered error " + e.getMessage());
					 throw e;
				}
				if (sessionPath != null && sessionName != null) {
				 LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher(null,null);
				 localProcessLauncher.launchProcess(new CommandStatementPresenter("ln -s " + sessionPath + "  " + sessionName), workingFolder, -1);
				}
			}catch(Exception pe) {
				 logger.debug(pe);
				 System.out.println("Session " + sessionIds.get(i) + " Encountered error " + pe.getMessage());
				 throw pe;
			 }
		}
	}

    private String getArchivePathForSession(MRSessionDocument mrSessionDoc) {
    	return FileUtils.getRelativePath(mrSessionDoc);
    }
	
	private ArrayList getValuesForParameter() throws PipelineException{
		ArrayList rtn = new ArrayList();
		String parameterFile = commandLineArgs.get("parameter-file");
		try {
	            XmlObject xmlObject = new XmlReader().read(parameterFile);
	            if (!(xmlObject instanceof ParametersDocument)) {
	                logger.error("addParameter() :: Invalid XML file supplied. Expecting a parameter document"); 
	                throw new PipelineException("Invalid XML file supplied " + parameterFile + " ==> Expecting a parameters document");
	            }
	            ParametersDocument parameterDoc = (ParametersDocument)xmlObject; 
	            String errors = XMLBeansUtils.validateAndGetErrors(parameterDoc);
	            if (errors != null) {
	                throw new XmlException(" Invalid XML " + parameterFile + "\n" + errors);
	            }
	            rtn = XPathResolverSaxon.GetInstance().resolveXPathExpressions("/Parameters/parameter[name='" + commandLineArgs.get("parameter-name") +"']/values/list",parameterDoc);
	            if (rtn == null || rtn.size() == 0) {
		            rtn = XPathResolverSaxon.GetInstance().resolveXPathExpressions("/pip:Parameters/pip:parameter[pip:name='" + commandLineArgs.get("parameter-name") +"']/pip:values/pip:list",parameterDoc);
	            }
	            if (rtn == null || rtn.size() == 0) {
	            	rtn = XPathResolverSaxon.GetInstance().resolveXPathExpressions("/Parameters/parameter[name='" + commandLineArgs.get("parameter-name") +"']/values/unique",parameterDoc);
		            if (rtn == null || rtn.size() == 0) {
		            	rtn = XPathResolverSaxon.GetInstance().resolveXPathExpressions("/pip:Parameters/pip:parameter[pip:name='" + commandLineArgs.get("parameter-name") +"']/pip:values/pip:unique",parameterDoc);
		            }
	            }
		}catch(IOException ioe) {
	            logger.error("File not found " + parameterFile);
	            throw new PipelineException(ioe.getClass() + "==>" + ioe.getLocalizedMessage(), ioe);
	        }catch (XmlException xmle ) {
	            logger.error(xmle.getLocalizedMessage());
	            throw new PipelineException(xmle.getClass() + "==>" + xmle.getLocalizedMessage(),xmle);
	        }catch(PipelineException ane) {
	            ane.printStackTrace();
	            logger.error(ane.getLocalizedMessage());
	            throw new PipelineException(ane.getClass() + "==>" + ane.getLocalizedMessage(),ane);
	        }
	        return rtn;
	}
	
    public void printUsage() {
        String usage = "LsList: Invokes unix ln command to a specified folder within the archive for each variable in a list \n";
        usage += "Options:\n";
        usage += "\t --parameter-file -p: <Parameter file>\n";
        usage += "\t --parameter-name -n: <Name of the parameter which has the XNAT Session Ids> \n";
        usage += "\t --working-dir -d: <Directory into which Softlinks are to be created> \n";
        usage += "\t --from-subdir -f: <The Subdir within the Session Id archive dir to which a softlink is to be created>\n";
       // usage += "\t --to-subdir -t: <The Subdir within the Working dir in the which the soft links are to be created>\n";
        usage += "\t --host -o: <XNAT Host>\n";
        usage += "\t --user -u: XNAT username  \n";
        usage += "\t --pwd -w: XNAT password\n";
        usage += "\t --log -l <path to log4j.properties file>\n";
        usage += "\t -help\n";

        System.out.println(usage);
        System.exit(1);
    }

    public String getLogPropertiesFile() {
        return (String)commandLineArgs.get("log");
    }
    
	public static void main(String args[]) {
		 try {
	            LnList lnList = new LnList(args);
	            if (lnList.getLogPropertiesFile() != null) {
	                PropertyConfigurator.configure(lnList.getLogPropertiesFile());
	            }else {
	                BasicConfigurator.configure();
	            }
	            lnList.launch();
	            System.exit(0);
	        }catch(Exception e) {
	            e.printStackTrace();
	            System.exit(1);
	        }
	}
	
    private static Logger logger = Logger.getLogger(LnList.class);

    private int noOfRequiredArguments = 7;
    private int noOfRequiredArgumentsAvailable = 0;
	private Hashtable<String,String> commandLineArgs;


}
