package org.nrg.pipeline.client;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.exception.PipelineException;
import org.nrg.pipeline.manager.EventManager;
import org.nrg.pipeline.manager.PipelineManager;
import org.nrg.pipeline.utils.ExceptionUtils;
import org.nrg.pipeline.utils.MailUtils;
import org.nrg.pipeline.utils.Notification;
import org.nrg.pipeline.utils.ParameterUtils;
import org.nrg.pipeline.utils.PipelineProperties;
import org.nrg.pipeline.xmlbeans.PipelineData.Parameters;
import org.nrg.pipeline.xmlbeans.workflow.AbstractExecutionEnvironment;
import org.nrg.pipeline.xmlbeans.workflow.WorkflowData;
import org.nrg.pipeline.xmlbeans.workflow.WorkflowDocument;
import org.nrg.pipeline.xmlbeans.workflow.XnatExecutionEnvironment;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnattools.xml.XMLSearch;
import org.nrg.xnattools.xml.XMLStore;

public class XNATPipelineLauncher implements Observer {
    
    WorkflowDocument workFlow = null;
    XnatExecutionEnvironment execEnv = null;
    Properties properties;
    
    public XNATPipelineLauncher(CommandLineArguments args) {
        commandLineArgs = args;
        execEnv =commandLineArgs.getExecutionEnvironment();
    }

    public Parameters launch() throws Exception {
          setProperties();
          PipelineProperties.init(properties);
          isPipelineQueuedOrAwaitingOrOnHold();
          EventManager.GetInstance().addObserver(this);
          Parameters params = PipelineManager.GetInstance(commandLineArgs.getConfigurationFile()).launchPipeline(commandLineArgs.getPipelineFullPath(),commandLineArgs.getParametersDocument(), commandLineArgs.getStartAt(), false);
          return params;
    }
    
    private void setProperties() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(commandLineArgs.getConfigurationFile()));
        }catch(Exception e){
            System.out.println("Couldnt read the properties file " + commandLineArgs.getConfigurationFile() +"....aborting" );
            logger.fatal("Couldnt read the properties file " + commandLineArgs.getConfigurationFile() +"....aborting" );
            System.exit(1);
        }
        if (properties.size() == 0) {
            System.out.println("Configuration file " +  commandLineArgs.getConfigurationFile() +" doesnt have any properties specified .... aborting");
            System.exit(1);
        }
    }

    private Properties getProperties() {
        return properties;
    }
    
    
    public void assignGridJobIdToWorkflow(String jobId) {
        isPipelineQueuedOrAwaitingOrOnHold();
        WorkflowDocument wrkFlow = WorkflowDocument.Factory.newInstance();
        WorkflowData workFlowData = wrkFlow.addNewWorkflow();
        if (workFlow != null) {
            workFlowData.set(workFlow.getWorkflow());
        }else {
            workFlowData.setLaunchTime(Calendar.getInstance());
        	workFlowData.setDataType(commandLineArgs.getDataType());
            workFlowData.setID(commandLineArgs.getId());
            workFlowData.setPipelineName(commandLineArgs.getPipelineName());
            workFlowData.setStatus("HOLD");
            if (commandLineArgs.getProject() != null)
            	workFlowData.setExternalID(commandLineArgs.getProject());
        }
        workFlowData.setJobID(jobId);
        try {
        	new XMLStore(wrkFlow,commandLineArgs.getHost(), commandLineArgs.getUserName(), commandLineArgs.getPassword()).store();
        }catch(Exception e) {
        	logger.fatal(e);
        }
    }
    
    public void assignGridJobIdToWorkflow(String jobId, String aliasHostUrl) {
        isPipelineQueuedOrAwaitingOrOnHold(aliasHostUrl);
        WorkflowDocument wrkFlow = WorkflowDocument.Factory.newInstance();
        WorkflowData workFlowData = wrkFlow.addNewWorkflow();
        if (workFlow != null) {
            workFlowData.set(workFlow.getWorkflow());
        }else {
            workFlowData.setLaunchTime(Calendar.getInstance());
        	workFlowData.setDataType(commandLineArgs.getDataType());
            workFlowData.setID(commandLineArgs.getId());
            workFlowData.setPipelineName(commandLineArgs.getPipelineName());
            workFlowData.setStatus("HOLD");
            if (commandLineArgs.getProject() != null)
            	workFlowData.setExternalID(commandLineArgs.getProject());
        }
        workFlowData.setJobID(jobId);
        try {
        	new XMLStore(wrkFlow,aliasHostUrl, commandLineArgs.getUserName(), commandLineArgs.getPassword()).store();
        }catch(Exception e) {
        	logger.fatal(e);
        }
    }
    
    private void isPipelineQueuedOrAwaitingOrOnHold() {
        try {
            XMLSearch search = new XMLSearch(commandLineArgs.getHost(), commandLineArgs.getUserName(), commandLineArgs.getPassword() );
            ArrayList<String> files = search.searchAll("wrk:workflowData.ID",commandLineArgs.getId(),"=","wrk:workflowData",FileUtils.getTempFolder());
        	for (int i = 0; i < files.size(); i++) {
	           WorkflowDocument wrkFlow = (WorkflowDocument)new XmlReader().read(files.get(i), true);
	           if (wrkFlow.getWorkflow().getPipelineName().equals(commandLineArgs.getPipelineName()) && ( wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("QUEUED") ||wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("AWAITING ACTION")|| wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("HOLD") )) {
	               workFlow = wrkFlow;
	               break;
	           }
	        }
        }catch(Exception e) {
          logger.fatal("Couldnt search for queued workflows", e);   
        }
    }
    
    private void isPipelineQueuedOrAwaitingOrOnHold(String aliasHost) {
        try {
            XMLSearch search = new XMLSearch( aliasHost, commandLineArgs.getUserName(), commandLineArgs.getPassword() );
            ArrayList<String> files = search.searchAll("wrk:workflowData.ID",commandLineArgs.getId(),"=","wrk:workflowData",FileUtils.getTempFolder());
        	for (int i = 0; i < files.size(); i++) {
	           WorkflowDocument wrkFlow = (WorkflowDocument)new XmlReader().read(files.get(i), true);
	           if (wrkFlow.getWorkflow().getPipelineName().equals(commandLineArgs.getPipelineName()) && ( wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("QUEUED") ||wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("AWAITING ACTION")|| wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("HOLD") )) {
	               workFlow = wrkFlow;
	               break;
	           }
	        }
        }catch(Exception e) {
          logger.fatal("Couldnt search for queued workflows", e);   
        }
    }
    
    public synchronized void update(Observable obj, Object msg) {
        if (msg instanceof Notification) {
            Notification notification = (Notification)msg;
            if (notification == null) return;
                WorkflowDocument wrkFlow = WorkflowDocument.Factory.newInstance();
                WorkflowData workFlowData = wrkFlow.addNewWorkflow();
                if (workFlow != null) {
                    workFlowData.set(workFlow.getWorkflow());
                }else {
                    workFlowData.setLaunchTime(notification.getPipelineTimeLaunched());
                }
                
                workFlowData.setDataType(commandLineArgs.getDataType());
                workFlowData.setID(commandLineArgs.getId());
                workFlowData.setExternalID(commandLineArgs.getProject());
                if (notification.getStepTimeLaunched()!=null)
                    workFlowData.setCurrentStepLaunchTime(notification.getStepTimeLaunched());
                if (notification.getCurrentStep()!=null)
                    workFlowData.setCurrentStepId(notification.getCurrentStep());
                if (notification.getStatus()!=null)
                    workFlowData.setStatus(notification.getStatus());
                workFlowData.setPipelineName(commandLineArgs.getPipelineName());
                if (notification.getNextStep()!=null)
                    workFlowData.setNextStepId(notification.getNextStep());
                workFlowData.setStepDescription(notification.getMessage());
                workFlowData.setPercentageComplete(""+notification.getPercentageStepsCompleted());
                if (workFlowData != null && !workFlowData.isSetExecutionEnvironment()) {
                   AbstractExecutionEnvironment absExec = workFlowData.addNewExecutionEnvironment();
                   XnatExecutionEnvironment aexecEnv = (XnatExecutionEnvironment)absExec.changeType(XnatExecutionEnvironment.type);
                   aexecEnv.set(execEnv);
                }
                try {
                	new XMLStore(wrkFlow,commandLineArgs.getHost(), commandLineArgs.getUserName(), commandLineArgs.getPassword()).store();
                }catch(Exception e) {
                	System.out.println("Unable to store workflow entry. Host " + commandLineArgs.getHost() + " may not be accessible? " );
                	logger.fatal(e);
                	System.exit(1);
                }
        }
    }
    
    public void notify(boolean doesNotification) {
        commandLineArgs.setSupressNotification(doesNotification);
    }
    
    public boolean doesNotification() {
        return !commandLineArgs.isSupressNotification();
    }
    
    public void log(String argv[]) {
        if (argv == null) return;
        for (int i=0; i < argv.length; ) {
            if (argv[i].equals("-pwd")) {
                logger.info(argv[i] + "*******");
                i+=2;
            }else if (argv[i].equals("-parameter") && argv[i+1].startsWith("pwd")) {
                logger.info(argv[i] + "pwd=*******");
                i+=2;
            }else {
                logger.info(argv[i] + " ");
                i++;
            }
        }
        logger.info("====================================\n");
    }
    
    public void composeFailureMessage(Exception e) {
        String site = properties.getProperty("XNAT_SITE");
        if (site == null) site = "XNAT";
        String adminEmail = properties.getProperty("ADMIN_EMAIL");
        if (adminEmail == null) adminEmail = "Site Manager";
        String[] adminEmails = adminEmail.split(",");
        String msg = "The following problems were encountered by the user(s) <br> ";
        ArrayList notifyEmails = commandLineArgs.getEmailIds();
        if (notifyEmails != null ) {
            for (int i = 0; i < notifyEmails.size(); i++) {
                msg += notifyEmails.get(i) + " ";
            }
        }
        msg += "<br>";
        String label = commandLineArgs.getLabel();
        if (label == null) label = commandLineArgs.getId();
        String outfilepath = null;
        String errorfilepath = null;
        if (e instanceof PipelineException) {
        	outfilepath = ((PipelineException)e).getOutputFileName();
        	errorfilepath = ((PipelineException)e).getErrorFileName();
        }
        String failureMsg = "The processing request you submitted for " +  label + " could not be completed at this time. <br><br><br> The " + site + " technical team is aware of the issue and will notify you when it has been resolved.";
        failureMsg += "<br><br><br> The stdout and the error log files are available as attachments for your perusal.";
        failureMsg += "<br><br><br> We appreciate your patience.  Please contact " +  adminEmails[0] + " with questions or concerns. ";
        try {
        	if (!commandLineArgs.notifyOnlyAdmin())
        		MailUtils.send(site + " update: Processing failed for " + label, failureMsg , commandLineArgs.getEmailIds(), outfilepath, errorfilepath);
            failureMsg += "<br><br><br> Pipeline:  <br><br>" + commandLineArgs.getPipelineFullPath();
            failureMsg += "<br><br><br> Cause:  <br><br>" + ExceptionUtils.getStackTrace(e, null); 
            if (adminEmail != null) {
                ArrayList emails = new ArrayList(); 
                for(int l=0; l< adminEmails.length; l++) emails.add(adminEmails[l]);
                MailUtils.send(site + " update: Processing failed for " + label, failureMsg ,emails,  outfilepath, errorfilepath);
            }
        }catch(Exception e1) {
            System.out.println("Couldnt send email msg");
        }
    }
    
    public static void main(String argv[]) {
        CommandLineArguments commandArgs = new CommandLineArguments(argv);
        if (commandArgs.getLogPropertiesFile() != null) {
            PropertyConfigurator.configure(commandArgs.getLogPropertiesFile());
        }else {
            BasicConfigurator.configure();
        }
        XNATPipelineLauncher launcher = new XNATPipelineLauncher(commandArgs);
        launcher.log(argv);
        try {
            Parameters params = launcher.launch();
            if (launcher.doesNotification() && commandArgs.getEmailIds() != null && commandArgs.getEmailIds().size() > 0) {
                try {
                    MailUtils.send("Pipeline Complete", "Pipeline: " + commandArgs.getPipelineFullPath() + " was succesfully completed for " + ParameterUtils.GetParameters(params) , commandArgs.getEmailIds(), null, null);
                }catch (Exception e1) {
                    System.out.println("Couldnt send email msg");
                    e1.printStackTrace();
                    System.exit(1);
                }
            }
            System.exit(0);
        }catch(Exception e) {
            e.printStackTrace();
            System.out.println("Unable to launch pipeline " + commandArgs.getPipelineFullPath());
            try {
                launcher.composeFailureMessage(e);
            }catch (Exception e1) {
                System.out.println("Couldnt send email msg");
                e1.printStackTrace();
            }
            System.exit(1);
        }

        
    }

    
    CommandLineArguments commandLineArgs;
    static Logger logger = Logger.getLogger(XNATPipelineLauncher.class);
}
