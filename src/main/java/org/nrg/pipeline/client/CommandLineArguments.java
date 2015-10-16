/*
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 *
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client;

import com.Ostermiller.util.CSVParser;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import org.apache.log4j.Logger;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlOptions;
import org.nrg.pipeline.utils.MailUtils;
import org.nrg.pipeline.utils.XMLBeansUtils;
import org.nrg.pipeline.xmlbeans.ParameterData;
import org.nrg.pipeline.xmlbeans.ParameterData.Values;
import org.nrg.pipeline.xmlbeans.ParametersDocument;
import org.nrg.pipeline.xmlbeans.ParametersDocument.Parameters;
import org.nrg.pipeline.xmlbeans.workflow.XnatExecutionEnvironment;
import org.nrg.pipeline.xmlreader.XmlReader;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class CommandLineArguments extends AbsVersion {

    /**
     * @return Returns the supressNotification.
     * If a pipeline has a notification step then we dont want XNATPipelineLauncher to send an email.
     */
    public boolean isSupressNotification() {
        return supressNotification;
    }

    /**
     * @return Returns the notifyonlyadmin
     * This flag decides if failure messages should be sent to user or only admin .
     */
    public boolean notifyOnlyAdmin() {
        return notifyonlyadmin;
    }

    /**
     * @param supressNotification The supressNotification to set.
     */
    public void setSupressNotification(boolean supressNotification) {
        this.supressNotification = supressNotification;
    }

    /**
     * Indicates whether workflow entries should be recorded for the current pipeline.
     * @return <b>true</b> (the default) if workflow entries should be recorded, <b>false</b> otherwise.
     */
    public boolean recordWorkflow() {
        return recordWorkflow;
    }

    @SuppressWarnings("unused")
    public void setRecordWorkflow(boolean recordWorkflow) {
        this.recordWorkflow = recordWorkflow;
    }

    public Integer getWorkFlowPrimaryKey() {
        return workFlowPrimaryKey;
    }


    public CommandLineArguments(String argv[]) {
        args = argv;
        int c;
        commandLineArgs = new Hashtable<String, Object>();
        execEnv = XnatExecutionEnvironment.Factory.newInstance();
        params = ParametersDocument.Parameters.Factory.newInstance();
        hasParamFile = false;
        paramsFromFileDoc = null;

        List<LongOpt> longopts = new ArrayList<LongOpt>();
        longopts.add(new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'));
        longopts.add(new LongOpt("pipeline", LongOpt.REQUIRED_ARGUMENT, null, 'p'));
        longopts.add(new LongOpt("parameter", LongOpt.REQUIRED_ARGUMENT, null, 'r'));
        longopts.add(new LongOpt("parameterFile", LongOpt.REQUIRED_ARGUMENT, null, 'm'));
        longopts.add(new LongOpt("startAt", LongOpt.REQUIRED_ARGUMENT, null, 's'));
        longopts.add(new LongOpt("notify", LongOpt.REQUIRED_ARGUMENT, null, 'e'));
        longopts.add(new LongOpt("dataType", LongOpt.REQUIRED_ARGUMENT, null, 'd'));
        longopts.add(new LongOpt("id", LongOpt.REQUIRED_ARGUMENT, null, 'i'));
        longopts.add(new LongOpt("debug", LongOpt.NO_ARGUMENT, null, 'g'));
        longopts.add(new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'y'));
        longopts.add(new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'w'));
        longopts.add(new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o'));
        longopts.add(new LongOpt("supressNotification", LongOpt.NO_ARGUMENT, null, 'n'));
        longopts.add(new LongOpt("log", LongOpt.REQUIRED_ARGUMENT, null, 'l'));
        longopts.add(new LongOpt("catalogPath", LongOpt.REQUIRED_ARGUMENT, null, 'c'));
        longopts.add(new LongOpt("config", LongOpt.REQUIRED_ARGUMENT, null, 'f'));
        longopts.add(new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'v'));
        longopts.add(new LongOpt("project", LongOpt.REQUIRED_ARGUMENT, null, 't'));
        longopts.add(new LongOpt("aliasHost", LongOpt.REQUIRED_ARGUMENT, null, 'a'));
        longopts.add(new LongOpt("label", LongOpt.REQUIRED_ARGUMENT, null, 'b'));
        longopts.add(new LongOpt("useAlias", LongOpt.NO_ARGUMENT, null, 'z'));
        longopts.add(new LongOpt("notifyonlyadmin", LongOpt.NO_ARGUMENT, null, 'j'));
        longopts.add(new LongOpt("recordWorkflow", LongOpt.OPTIONAL_ARGUMENT, null, 'x'));
        longopts.add(new LongOpt("workFlowPrimaryKey", LongOpt.REQUIRED_ARGUMENT, null, 'k'));

        //
        Getopt g = new Getopt("XNATPipelineLauncher", argv, "p:r:m:s:e:d:i:y:w:o:l:c:f:t:a:b:zjghnvx;", longopts.toArray(new LongOpt[longopts.size()]), true);
        g.setOpterr(false); // We'll do our own error handling
        //
        int noOfRequiredArgumentsAvailable = 0;
        while ((c = g.getopt()) != -1) {
            switch (c)
            {
                case 'p':
                    commandLineArgs.put("pipeline",g.getOptarg());
                    execEnv.setPipeline(g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'r':
                    addParameter(g.getOptarg(), false);
                    break;
                case 'm':
                    hasParamFile = true;
                    readParameterDocument(g.getOptarg());
                    break;
                case 's':
                    execEnv.setStartAt(g.getOptarg());
                    commandLineArgs.put("startAt",g.getOptarg());
                    break;
                case 'e':
                    execEnv.addNotify(g.getOptarg());
                    addEmail(g.getOptarg());
                    break;
                case 'd':
                    execEnv.setDataType(g.getOptarg());
                    commandLineArgs.put("dataType",g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 't':
                    String project = g.getOptarg();
                    commandLineArgs.put("project",project);
                    addParameter("project="+project, false);
                    break;
                case 'i':
                    execEnv.setId(g.getOptarg());
                    commandLineArgs.put("id",g.getOptarg());
                    addParameter("id="+g.getOptarg(), false);
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'f':
                    //execEnv.setConfigurationFile(g.getOptarg());
                    commandLineArgs.put("config",g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'y':
                    String username = g.getOptarg();
                    execEnv.setXnatuser(username);
                    commandLineArgs.put("username",username);
                    ParameterData pData = params.addNewParameter();
                    pData.setName("user");
                    pData.addNewValues().setUnique(username);
                    addParameter("u="+username, false);
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'a':
                    //execEnv.setDataType(g.getOptarg());
                    commandLineArgs.put("aliasHost",g.getOptarg());
                    addParameter("aliasHost="+g.getOptarg(), false);
                    break;
                case 'z':
                    //execEnv.setDataType(g.getOptarg());
                    commandLineArgs.put("useAlias","TRUE");
                    addParameter("useAlias=TRUE", false);
                    break;
                case 'w':
                    String pwd = g.getOptarg();
                    commandLineArgs.put("password",pwd);
                    pData = params.addNewParameter();
                    pData.setName("pwd");
                    pData.addNewValues().setUnique(pwd);
                    addParameter("pwd="+pwd, true);
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'o':
                    String host = g.getOptarg();
                    execEnv.setHost(host);
                    if (!host.endsWith("/")) host+="/";
                    commandLineArgs.put("host",host);
                    addParameter("host="+host, false);
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'g':
                    commandLineArgs.put("debug", Boolean.TRUE.toString());
                    break;
                case 'h':
                    printUsage();
                    break;
                case 'n':
                    supressNotification = true;
                    break;
                case 'l':
                    //execEnv.setLog(g.getOptarg());
                    commandLineArgs.put("log",g.getOptarg());
                    break;
                case 'c':
                    //execEnv.setCatalogPath(g.getOptarg());
                    commandLineArgs.put("catalogPath",g.getOptarg());
                    break;
                case 'b':
                    //execEnv.setCatalogPath(g.getOptarg());
                    commandLineArgs.put("label",g.getOptarg());
                    addParameter("label="+g.getOptarg(), false);
                    break;
                case 'j':
                    //execEnv.setCatalogPath(g.getOptarg());
                    notifyonlyadmin = true;
                    break;
                case 'v':
                    echoVersion();
                    System.exit(0);
                case 'x':
                    recordWorkflow = Boolean.parseBoolean(g.getOptarg());
                    break;
                case 'k':
                    workFlowPrimaryKey = Integer.parseInt(g.getOptarg());
                    addParameter("workflowid="+workFlowPrimaryKey, false);
                    break;
                default:
                    echoVersion();
                    printUsage();
                    break;
            }
        }

        final int noOfRequiredArguments = 7;
        if (noOfRequiredArgumentsAvailable < noOfRequiredArguments) {
            System.out.println("Missing required arguments");
            printUsage();
        }
        execEnv.setSupressNotification(supressNotification);

        MailUtils.setMailService(this.getHost());
    }

    public XnatExecutionEnvironment getExecutionEnvironment() {
        return execEnv;
    }

    @SuppressWarnings("unchecked")
    public void addEmail(String emailId) {
        if (commandLineArgs.containsKey("notify")) {
            ((ArrayList<String>) commandLineArgs.get("notify")).add(emailId);
        } else {
            ArrayList<String> emails = new ArrayList<String>();
            emails.add(emailId);
            commandLineArgs.put("notify", emails);
        }
    }

    public void printUsage() {
        String usage = "XNATPipelineLauncher  \n";
        usage += "Options:\n";
        usage += "\t -log <path to log4j.properties file>\n";
        usage += "\t -pipeline <path to pipeline xml file>\n";
        usage += "\t -config: Properties configuration file\n";
        usage += "\t -parameter: FORMAT: <param name>=<comma separated values> \n";
        usage += "\t\t eg: -parameter mpr=4,5,6\n";
        usage += "\t -parameterFile: Path to parameters xml \n";
        usage += "\t\t eg: -parameterFile /data/analysis/Parameters.xml\n";
        usage += "\t -dataType: XNAT Data type for which pipeline has been launched\n";
        usage += "\t -id: XNAT ID which uniquely identifies the dataType\n";
        usage += "\t -label: XNAT Label for the  dataType\n";
        usage += "\t -project: XNAT Project to which this id belongs \n";
        usage += "\t -u: XNAT username \n";
        usage += "\t -pwd: XNAT password\n";
        usage += "\t -host: URL to XNAT based Website\n";
        usage += "\t -aliasHost: URL to XNAT based Alias Website\n";
        usage += "\t -useAlias: Force to use aliasHost and not host\n";
        usage += "\t -startAt: (optional) Step to start pipeline at -- \n";
        usage += "\t -catalogPath: Root path relative to which Pipeline XML's are located\n";
        usage += "\t -notify: (optional) Email Ids to which notifications are to be sent\n";
        usage += "\t -supressNotification: (optional) Pipeline completion emails will be supressed\n";
        usage += "\t -workFlowPrimaryKey <Integer value>: (preferred) Workflow entry will be updated using the primary key\n";
        usage += "\t -help\n";

        System.out.println(usage);
        System.exit(1);
    }

    /**
     * @return Returns the dataType.
     */
    public String getDataType() {
        //System.out.println("Datatype is " + commandLineArgs.get("dataType"));
        return (String)commandLineArgs.get("dataType");
    }

    public String getProject() {
        //System.out.println("Datatype is " + commandLineArgs.get("dataType"));
        return (String)commandLineArgs.get("project");
    }

    /**
     * @return Returns the Properties configuration file
     */
    public String getConfigurationFile() {
        return (String)commandLineArgs.get("config");
    }

    /**
     * @return Returns the id.
     */
    public String getId() {
        return (String)commandLineArgs.get("id");
    }

    /**
     * @return Returns the XNAT Label.
     */
    public String getLabel() {
        return (String)commandLineArgs.get("label");
    }
    /**
     * @return Returns the parameters.
     */
    public ParametersDocument getParametersDocument() {
        ParametersDocument paramDoc = ParametersDocument.Factory.newInstance();
        paramDoc.setParameters(params);
        Parameters param = paramDoc.getParameters();
        if (hasParamFile) {
            Parameters paramsFromFile = paramsFromFileDoc.getParameters();
            for (int i = 0; i < paramsFromFile.sizeOfParameterArray(); i++) {
                param.addNewParameter().set(paramsFromFile.getParameterArray(i));
            }
        }
        return paramDoc;
    }

    public String getPipelineName() {
        return (String)commandLineArgs.get("pipeline");
    }

    /**
     * @return Returns the pipelineName.
     */

    public String getPipelineFullPath() {
        String rootPath = (String)commandLineArgs.get("catalogPath");
        if (rootPath != null) {
            if (!rootPath.endsWith(File.separator))
                rootPath += File.separator;
        }
        String pipelineRelativePath = (String)commandLineArgs.get("pipeline");
        if (File.separator.equals("/")) {
            if (pipelineRelativePath.startsWith("/"))
                return pipelineRelativePath;
            else
                return rootPath + pipelineRelativePath;
        }else{
            if (pipelineRelativePath.contains(":\\")) {
                return pipelineRelativePath;
            }else if (pipelineRelativePath.contains(":/"))
                return pipelineRelativePath;
            else
                return rootPath + pipelineRelativePath;
        }
    }

    /**
     * @return Returns the startAt.
     */
    public String getStartAt() {
        return (String)commandLineArgs.get("startAt");
    }

    /**
     * @return Returns the host.
     */
    public String getHost() {
        String rtn =(String)commandLineArgs.get("aliasHost");
        String usealias = (String)commandLineArgs.get("useAlias");
        if (rtn == null)
            rtn = (String)commandLineArgs.get("host");
        else {
            String pipelineName = getPipelineName().trim();
            if (pipelineName.endsWith("Transfer.xml") || pipelineName.endsWith("AutoRun.xml") ) {
                return rtn;
            }else if (usealias !=null && usealias.equalsIgnoreCase("TRUE")) {
                return rtn;
            }else
                return (String)commandLineArgs.get("host");
        }
        return rtn;
    }

    @SuppressWarnings("unused")
    public String getHost(boolean alias) {
        String rtn =(String)commandLineArgs.get("aliasHost");
        if (rtn == null)
            rtn = (String)commandLineArgs.get("host");
        else {
            String pipelineName = getPipelineName().trim();
            if (pipelineName.endsWith("Transfer.xml")) {
                return rtn;
            }else  {
                if (alias)
                    return (String)commandLineArgs.get("aliasHost");
                else
                    return (String)commandLineArgs.get("host");
            }
        }
        return rtn;
    }


    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return (String)commandLineArgs.get("password");
    }

    /**
     * @return Returns the userName.
     */
    public String getUserName() {
        return (String)commandLineArgs.get("username");
    }

    @SuppressWarnings("unchecked")
    public ArrayList<String> getEmailIds() {
        return ((ArrayList<String>) commandLineArgs.get("notify"));
    }

    public String getLogPropertiesFile() {
        return (String)commandLineArgs.get("log");
    }

    public void logCommandLineArguments() {
        if (args == null || args.length == 0) {
            logger.debug("No command-line arguments found.");
            return;
        }
        final StringBuilder output = new StringBuilder("Pipeline executed with command line: ");
        boolean isPassword = false;
        for (final String arg : args) {
            if (arg.equals("-pwd") || arg.equals("-w")) {
                isPassword = true;
                output.append(arg);
            } else if (isPassword) {
                output.append("XXXXXXXXX");
                isPassword = false;
            } else {
                output.append(arg);
            }
            output.append(" ");
        }
        logger.debug(output.toString().trim());
    }

    private void addParameter(String paramValuePair, boolean sensitive) {
        //expected to get <name>=<csv value>
        paramValuePair = paramValuePair.trim();
        if (!sensitive) System.out.println("Param Value Pair " + paramValuePair);
        String parts[] = paramValuePair.split("=");
        if (parts.length < 2) {
            System.out.println("Invalid parameter found: " + paramValuePair);
            printUsage();
            System.exit(1);
        }
        String paramName = parts[0].trim();
        String paramValues = parts[1].trim();
        if (sensitive) System.out.println("Param Value Pair " + paramName + "=********");

        if (!paramName.equals("u") && !paramName.equals("pwd") && !paramName.equals("host")) {
            XnatExecutionEnvironment.Parameters  execParams = execEnv.getParameters();
            if (!execEnv.isSetParameters()) {
                execParams = execEnv.addNewParameters();
            }
            XnatExecutionEnvironment.Parameters.Parameter execParam =  execParams.addNewParameter();
            execParam.setName(paramName);
            execParam.setStringValue(paramValues);
        }
        ParameterData pData = params.addNewParameter();
        pData.setName(paramName);
        Values values = pData.getValues();
        if (values == null) values = pData.addNewValues();
        String[][] str = CSVParser.parse(paramValues);
        if (str==null || str.length != 1) {
            System.out.println("Invalid parameter found: " + paramValuePair);
            System.out.println("NOTE: If a parameter includes a comma or a new line, the whole field must be surrounded with double quotes.");
            System.out.println("When the field is in quotes, any quote literals must be escaped by \" Backslash literals must be escaped by \\");
            System.out.println("Otherwise a backslash and the character following will be treated as the following character, IE. \"\n\" is equivalent to \"n\".");
            System.out.println("Text that comes after quotes that have been closed but come before the next comma will be ignored.");
            printUsage();
            System.exit(1);
        }
        if (str[0].length == 1) {
            values.setUnique(str[0][0]);
        }else {
            for (int i = 0; i < str[0].length; i++) {
                values.addList(str[0][i]);
            }
        }

        //parts = paramValues.split(",");
        //if (parts == null) {
        //    System.out.println("Invalid parameter found: " + paramValuePair);
        //    printUsage();
        //   System.exit(1);
        //}

/*        if (parts.length == 1) {
            values.setUnique(parts[0]);
        }else {
            for (int i = 0; i < parts.length; i++) {
                values.addList(parts[i]);
            }
        }
*/    }

    private void readParameterDocument(String path){
        try {
            paramsFromFileDoc = (ParametersDocument)new XmlReader().read(path, false);
            String err = XMLBeansUtils.validateAndGetErrors(paramsFromFileDoc);
            if (err != null) {
                throw new XmlException("Invalid XML " + path + "\n" + err);
            }

            XnatExecutionEnvironment.ParameterFile  execParamFile = execEnv.getParameterFile();
            if (!execEnv.isSetParameterFile()) {
                execParamFile = execEnv.addNewParameterFile();
            }
            execParamFile.setPath(path);
            if (!execParamFile.isSetXml()) {
                //execParamFile.setXml("<![CDATA[" + paramsFromFileDoc.xmlText(new XmlOptions().setSaveAggressiveNamespaces()) + "]]>");
                execParamFile.setXml(paramsFromFileDoc.xmlText(new XmlOptions().setSaveAggressiveNamespaces()));
            }
//            System.out.println(execEnv.toString());

            // System.out.println(execParamFile.getXml());
            /*            for (int i = 0; i < paramsFromFile.sizeOfParameterArray(); i++) {
                XnatExecutionEnvironment.Parameters.Parameter execParam =  execParams.addNewParameter();
                execParam.setName(paramsFromFile.getParameterArray(i).getName());
                String paramValues = "";
                Values values =  paramsFromFile.getParameterArray(i).getValues();
                if (values.isSetUnique()) {
                    execParam.setStringValue(values.getUnique());
                }else {
                    for (int j =0; j < values.sizeOfListArray(); j++) {
                        paramValues += values.getListArray(j) + ",";
                    }
                    if (paramValues.endsWith(",")) {
                        paramValues.substring(0,paramValues.length()-1);
                    }
                    execParam.setStringValue(paramValues);
                }
            }
*/        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final Logger logger = Logger.getLogger(CommandLineArguments.class);

    //boolean isRecon = false;
    private String[] args;
    private XnatExecutionEnvironment execEnv;
    private Map<String, Object> commandLineArgs;
    private Parameters params;
    private ParametersDocument paramsFromFileDoc;
    private boolean supressNotification = false;
    private boolean notifyonlyadmin = false;
    private boolean hasParamFile = false;
    private boolean recordWorkflow = true;
    private Integer workFlowPrimaryKey=null;
}