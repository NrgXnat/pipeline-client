package org.nrg.pipeline.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.exception.PipelineEngineException;
import org.nrg.pipeline.manager.EventManager;
import org.nrg.pipeline.manager.PipelineManager;
import org.nrg.pipeline.process.LocalProcessLauncher;
import org.nrg.pipeline.utils.CommandStatementPresenter;
import org.nrg.pipeline.utils.ExceptionUtils;
import org.nrg.pipeline.utils.MailUtils;
import org.nrg.pipeline.utils.Notification;
import org.nrg.pipeline.utils.ParameterUtils;
import org.nrg.pipeline.utils.PipelineProperties;
import org.nrg.pipeline.xmlbeans.ParameterData;
import org.nrg.pipeline.xmlbeans.PipelineData.Parameters;
import org.nrg.pipeline.xmlbeans.workflow.AbstractExecutionEnvironment;
import org.nrg.pipeline.xmlbeans.workflow.WorkflowData;
import org.nrg.pipeline.xmlbeans.workflow.WorkflowDocument;
import org.nrg.pipeline.xmlbeans.workflow.XnatExecutionEnvironment;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnattools.SessionManager;
import org.nrg.xnattools.service.WebServiceClient;
import org.nrg.xnattools.xml.WorkflowStore;
import org.nrg.xnattools.xml.XMLSearch;

public class XNATPipelineLauncher implements Observer {

    public XNATPipelineLauncher(List<String> args) {
        this(args.toArray(new String[args.size()]));
    }

    public XNATPipelineLauncher(String[] args) {
        this.args = args;
        commandLineArgs = new CommandLineArguments(args);

        init();
        execEnv = commandLineArgs.getExecutionEnvironment();
    }

    public XNATPipelineLauncher(CommandLineArguments args) {
        commandLineArgs = args;
        init();
        execEnv =commandLineArgs.getExecutionEnvironment();
    }

    private void init() {
        if (commandLineArgs.getLogPropertiesFile() != null) {
            PropertyConfigurator.configure(commandLineArgs.getLogPropertiesFile());
        } else {
            BasicConfigurator.configure();
        }
        //Acquire a JSESSION and this will be used throughout the execution of the pipeline
        SessionManager.GetInstance(commandLineArgs.getHost(), commandLineArgs.getUserName(), commandLineArgs.getPassword());
    }

    public Parameters launch() throws Exception {
        isPipelineQueuedOrAwaitingOrOnHold();
        EventManager.GetInstance().addObserver(this);
        Parameters params = PipelineManager.GetInstance(commandLineArgs.getConfigurationFile()).launchPipeline(commandLineArgs.getPipelineFullPath(), commandLineArgs.getParametersDocument(), commandLineArgs.getStartAt(), false);
        if (logger.isDebugEnabled()) {
            logger.debug("Found parameters: ");
            ParameterData[] array = params.getParameterArray();
            for (ParameterData item : array) {
                logger.debug(item.getName() + ": " + item.getValues().toString());
            }
        }
        return params;
    }

    public void assignGridJobIdToWorkflow(String jobId) {
        isPipelineQueuedOrAwaitingOrOnHold();
        WorkflowDocument wrkFlow = WorkflowDocument.Factory.newInstance();
        WorkflowData workFlowData = wrkFlow.addNewWorkflow();
        if (workFlow != null) {
            workFlowData.set(workFlow.getWorkflow());
        } else {
            workFlowData.setLaunchTime(Calendar.getInstance());
            workFlowData.setDataType(commandLineArgs.getDataType());
            workFlowData.setID(commandLineArgs.getId());
            workFlowData.setPipelineName(commandLineArgs.getPipelineName());
            workFlowData.setStatus("HOLD");
            if (commandLineArgs.getProject() != null) workFlowData.setExternalID(commandLineArgs.getProject());
        }
        workFlowData.setJobID(jobId);
        try {
            new WorkflowStore(wrkFlow, commandLineArgs.getHost(), commandLineArgs.getUserName(), commandLineArgs.getPassword()).store();
        } catch (Exception e) {
            logger.fatal(e);
        }
    }

    public void assignGridJobIdToWorkflow(String jobId, String aliasHostUrl) {
        isPipelineQueuedOrAwaitingOrOnHold(aliasHostUrl);
        WorkflowDocument wrkFlow = WorkflowDocument.Factory.newInstance();
        WorkflowData workFlowData = wrkFlow.addNewWorkflow();
        if (workFlow != null) {
            workFlowData.set(workFlow.getWorkflow());
        } else {
            workFlowData.setLaunchTime(Calendar.getInstance());
            workFlowData.setDataType(commandLineArgs.getDataType());
            workFlowData.setID(commandLineArgs.getId());
            workFlowData.setPipelineName(commandLineArgs.getPipelineName());
            workFlowData.setStatus("HOLD");
            if (commandLineArgs.getProject() != null) workFlowData.setExternalID(commandLineArgs.getProject());
        }
        workFlowData.setJobID(jobId);
        try {
            new WorkflowStore(wrkFlow, aliasHostUrl, commandLineArgs.getUserName(), commandLineArgs.getPassword()).store();
        } catch (Exception e) {
        	try {
            	wrkFlow.save(new File(commandLineArgs.getId()+"_wrk.xml"));
            } catch(IOException ioe) {
                logger.error("Error saving workflow to forensics file:\r" + wrkFlow.toString(), ioe);
            }
            logger.fatal("Unable to store workflow entry. Host " + commandLineArgs.getHost() + " may not be accessible?", e);
        }
    }

    public synchronized void update(Observable obj, Object msg) {

    	if (msg == null || !(msg instanceof Notification) || !commandLineArgs.recordWorkflow()) {
            return;
        }

        Notification notification = (Notification) msg;
        WorkflowDocument wrkFlow = WorkflowDocument.Factory.newInstance();
        WorkflowData workFlowData = wrkFlow.addNewWorkflow();
        if (workFlow != null) {
            workFlowData.set(workFlow.getWorkflow());
        } else {
            workFlowData.setLaunchTime(notification.getPipelineTimeLaunched());
        }

            workFlowData.setDataType(commandLineArgs.getDataType());
            workFlowData.setID(commandLineArgs.getId());
        workFlowData.setExternalID(commandLineArgs.getProject());

        if (notification.getStepTimeLaunched() != null) workFlowData.setCurrentStepLaunchTime(notification.getStepTimeLaunched());
        if (notification.getCurrentStep() != null) workFlowData.setCurrentStepId(notification.getCurrentStep());
        if (notification.getStatus() != null) workFlowData.setStatus(notification.getStatus());
        workFlowData.setPipelineName(commandLineArgs.getPipelineName());
        if (notification.getNextStep() != null) workFlowData.setNextStepId(notification.getNextStep());
        workFlowData.setStepDescription(notification.getMessage());
        workFlowData.setPercentageComplete("" + notification.getPercentageStepsCompleted());
        if (workFlowData != null && !workFlowData.isSetExecutionEnvironment()) {
            AbstractExecutionEnvironment absExec = workFlowData.addNewExecutionEnvironment();
            XnatExecutionEnvironment aexecEnv = (XnatExecutionEnvironment) absExec.changeType(XnatExecutionEnvironment.type);
            aexecEnv.set(execEnv);
        }

        try {
            new WorkflowStore(wrkFlow, commandLineArgs.getHost(), commandLineArgs.getUserName(), commandLineArgs.getPassword()).store();
        } catch (Exception e) {
            try {
                wrkFlow.save(new File(commandLineArgs.getId() + "_wrk.xml"));
            } catch (IOException ioe) {
                logger.error("Error saving workflow to forensics file:\r" + wrkFlow.toString(), ioe);
            }
            logger.fatal("Unable to store workflow entry. Host " + commandLineArgs.getHost() + " may not be accessible?", e);
            System.exit(1);
        }
    }

    public void notify(boolean doesNotification) {
        commandLineArgs.setSupressNotification(doesNotification);
    }

    public boolean doesNotification() {
        return !commandLineArgs.isSupressNotification();
    }

    public void log(String args[]) {
        if (args == null) return;
        for (int i = 0; i < args.length;) {
            if (args[i].equals("-pwd")) {
                logger.info(args[i] + "*******");
                i += 2;
            } else if (args[i].equals("-parameter") && args[i + 1].startsWith("pwd")) {
                logger.info(args[i] + "pwd=*******");
                i += 2;
            } else {
                logger.info(args[i] + " ");
                i++;
            }
        }
        logger.info("====================================\n");
    }

    public void composeFailureMessage(Exception e) {
        String site = getProperties().getProperty("XNAT_SITE");
        if (site == null) site = "XNAT";
        String adminEmail = getProperties().getProperty("ADMIN_EMAIL");
        if (adminEmail == null) adminEmail = "Site Manager";
        String[] adminEmails = adminEmail.split(",");
        StringBuilder message = new StringBuilder("The following problems were encountered by the user(s) <br> ");
        List<String> notifyEmails = commandLineArgs.getEmailIds();
        if (notifyEmails != null) {
            for (String notifyEmail : notifyEmails) {
                message.append(notifyEmail).append(" ");
            }
        }
        message.append("<br>");
        String label = commandLineArgs.getLabel();
        if (label == null) label = commandLineArgs.getId();
        String outfilepath = null;
        String errorfilepath = null;
        if (e instanceof PipelineEngineException) {
            outfilepath = ((PipelineEngineException) e).getOutputFileName();
            errorfilepath = ((PipelineEngineException) e).getErrorFileName();
        }
        message.append("The processing request you submitted for ").append(label).append(" could not be completed at this time. <br><br><br> The ").append(site).append(" technical team is aware of the issue and will notify you when it has been resolved.");
        message.append("<br><br><br> The stdout and the error log files are available as attachments for your perusal.");
        message.append("<br><br><br> We appreciate your patience.  Please contact ").append(adminEmails[0]).append(" with questions or concerns. ");

        if (outfilepath != null) {
        	String tailLines = getLastLines(outfilepath);
        	tailLines = tailLines.replace("\n", "<br>");
        	message.append("<br><br><br> TAIL ").append(outfilepath).append("<br><br> ").append(tailLines).append(" <br><br>");
        }
        if (errorfilepath != null) {
        	String tailLines = getLastLines(errorfilepath);
        	tailLines = tailLines.replace("\n", "<br>");
        	message.append("<br><br><br> TAIL ").append(errorfilepath).append("<br><br> ").append(tailLines).append(" <br><br>");
        }

        try {
            if (!commandLineArgs.notifyOnlyAdmin()) {
                MailUtils.send(site + " update: Processing failed for " + label, message.toString(), commandLineArgs.getEmailIds(), outfilepath, errorfilepath, commandLineArgs.getUserName(), commandLineArgs.getPassword());
            }
            message.append("<br><br><br> Pipeline:  <br><br>").append(commandLineArgs.getPipelineFullPath());
            message.append("<br><br><br> Cause:  <br><br>").append(ExceptionUtils.getStackTrace(e, null));
            ArrayList<String> emails = new ArrayList<String>();
            Collections.addAll(emails, adminEmails);
            MailUtils.send(site + " update: Processing failed for " + label, message.toString(), emails, outfilepath, errorfilepath,  commandLineArgs.getUserName(), commandLineArgs.getPassword());
        } catch (Exception e1) {
            logger.error("Couldn't send email message", e1);
        }
    }

    public static void main(String args[]) {
        CommandLineArguments commandArgs = new CommandLineArguments(args);
        XNATPipelineLauncher launcher = new XNATPipelineLauncher(commandArgs);
        boolean success = launcher.run();
        if (success) {
            System.exit(0);
        }
        System.exit(1);
    }


    private String getLastLines(String filePath) {
    	String rtn = "";
        String osName = System.getProperty("os.name");
    	try {
    		int tail_lines = 40;
    		try {
    			tail_lines = Integer.parseInt(getProperties().getProperty(FAILURE_EMAIL_INCLUDED_TAIL_LINES));
    		}catch(Exception ignored) {}

    		if (filePath != null && !osName.toLowerCase().startsWith("windows")) {
				CommandStatementPresenter command   = new CommandStatementPresenter("tail -n" + tail_lines + " " + filePath);
				LocalProcessLauncher launcher = new LocalProcessLauncher(null,null);
		        launcher.launchProcess(command,null, 1000);
		        if (launcher.getStreamOutput() != null)
		           rtn = launcher.getStreamOutput();
		        if (rtn == null && launcher.getStreamErrOutput() != null)
		           rtn = launcher.getStreamErrOutput();
		        return rtn;
    		}
    	}catch(Exception e) {
            logger.error("Error getting last lines from log", e);
    	}
    	return rtn;
    }

    public boolean run() {
        log(args);
        try {
            Parameters params = launch();
            success(params);
            return true;
        } catch (Exception e) {
            fail(e);
            return false;
        }finally {
            try {
            	SessionManager.GetInstance().deleteJSESSION();
            }catch(Exception e) {
            	e.printStackTrace();
            }
        }
    }

    private void fail(Exception e) {
        logger.error("Unable to launch pipeline " + commandLineArgs.getPipelineFullPath(), e);
        try {
            composeFailureMessage(e);
        } catch (Exception e1) {
            logger.error("Couldn't send email message", e1);
        }
    }

    private void success(Parameters params) {
        if (doesNotification() && commandLineArgs.getEmailIds() != null && commandLineArgs.getEmailIds().size() > 0) {
            try {
                MailUtils.send("Pipeline Complete", "Pipeline: " + commandLineArgs.getPipelineFullPath() + " was successfully completed for " + ParameterUtils.GetParameters(params), commandLineArgs.getEmailIds(), null, null, commandLineArgs.getUserName(), commandLineArgs.getPassword());
            } catch (Exception e1) {
                logger.error("Couldn't send email message", e1);
                System.exit(1);
            }
        }
    }

    private Properties getProperties() {
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(new FileInputStream(commandLineArgs.getConfigurationFile()));
            } catch (Exception e) {
                logger.fatal("Couldn't read the properties file " + commandLineArgs.getConfigurationFile() + "....aborting", e);
                System.exit(1);
            }
            if (properties.size() == 0) {
                logger.warn("Configuration file " + commandLineArgs.getConfigurationFile() + " doesn't have any properties specified .... aborting");
                System.exit(1);
            }
            PipelineProperties.init(properties);
        }
        return properties;
    }

    private void isPipelineQueuedOrAwaitingOrOnHold() {
    	isPipelineQueuedOrAwaitingOrOnHold(commandLineArgs.getHost());
    }

    private void isPipelineQueuedOrAwaitingOrOnHold(String aliasHost) {
        Integer workFlowPrimaryKey = commandLineArgs.getWorkFlowPrimaryKey();
    	if (workFlowPrimaryKey!=null) {
    		StringBuilder uri =  new StringBuilder("data/services/workflows/workflowid/");
            uri.append(workFlowPrimaryKey);
            final String concealHiddenFields = getProperties().getProperty(CONCEAL_HIDDEN_FIELDS);
            if (!StringUtils.isBlank(concealHiddenFields)) {
                uri.append("?concealHiddenFields=").append(Boolean.parseBoolean(concealHiddenFields));
            }
    		ByteArrayOutputStream out = new ByteArrayOutputStream();
    		ByteArrayInputStream in = null;
    		try {
	            WebServiceClient webClient = new WebServiceClient(aliasHost,commandLineArgs.getUserName(), commandLineArgs.getPassword());
	           	webClient.connect(uri.toString(), out);
	            in = new ByteArrayInputStream(out.toByteArray());
	            workFlow = (WorkflowDocument) new XmlReader().read(in);
    		} catch (Exception e) {
	            logger.fatal("Couldn't search for queued workflows", e);
	        }finally {
	        	try {
	        		out.close(); if (in != null) in.close();
	        	}catch(Exception e1){e1.printStackTrace();}
	        }
        }else {
	    	try {
	            XMLSearch search = new XMLSearch(aliasHost, commandLineArgs.getUserName(), commandLineArgs.getPassword());
	            ArrayList<String> files = search.searchAll("wrk:workflowData.ID", commandLineArgs.getId(), "=", "wrk:workflowData", FileUtils.getTempFolder());
                for (String file : files) {
                    WorkflowDocument wrkFlow = (WorkflowDocument) new XmlReader().read(file, true);
                    if (wrkFlow.getWorkflow().getPipelineName().equals(commandLineArgs.getPipelineName())
                            && (wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("QUEUED") || wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("AWAITING ACTION") || wrkFlow.getWorkflow().getStatus().equalsIgnoreCase("HOLD"))) {
                        workFlow = wrkFlow;
                        break;
                    }
                }
	        } catch (Exception e) {
	            logger.fatal("Couldn't search for queued workflows", e);
	        }
        }
    }

    private static final Logger logger = Logger.getLogger(XNATPipelineLauncher.class);
    private static final String FAILURE_EMAIL_INCLUDED_TAIL_LINES = "FAILURE_EMAIL_INCLUDED_TAIL_LINES";
    private static final String CONCEAL_HIDDEN_FIELDS = "CONCEAL_HIDDEN_FIELDS";

    private WorkflowDocument workFlow = null;
    private XnatExecutionEnvironment execEnv = null;
    private Properties properties;
    private CommandLineArguments commandLineArgs;
    private String[] args;

}
