/* *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.pet;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.xmlbeans.XmlCalendar;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlOptions;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.nrg.pipeline.client.utils.FileUtils;
import org.nrg.pipeline.stat.Statistics;
import org.nrg.pipeline.xmlbeans.cnda.PETTimeCourseDocument;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Durations;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Regions;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Durations.Duration;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Durations.Duration.Bp;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Regions.Region;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Regions.Region.TimeSeries;
import org.nrg.pipeline.xmlbeans.cnda.PetTimeCourseData.Regions.Region.TimeSeries.Activity;
import org.nrg.pipeline.xmlbeans.xnat.AbstractResource;
import org.nrg.pipeline.xmlbeans.xnat.ImageResource;
import org.nrg.pipeline.xmlbeans.xnat.PETSessionDocument;
import org.nrg.pipeline.xmlbeans.xnat.PetSessionData;
import org.nrg.pipeline.xmlbeans.xnat.RegionResource;
import org.nrg.pipeline.xmlbeans.xnat.Resource;
import org.nrg.pipeline.xmlbeans.xnat.ImageAssessorData.Out;
import org.nrg.pipeline.xmlbeans.xnat.RegionResource.Subregionlabels.Label;
import org.nrg.pipeline.xmlreader.XmlReader;
import org.nrg.xnattools.xml.XMLSearch;
import org.nrg.xnattools.xml.XMLStore;


public class PETTimeSeriesAssessorCreator {
    
    Hashtable<String,String> commandLineArgs;
    PETTimeCourseDocument petTimeCourseDoc = null;
    PETSessionDocument petSession = null;
    int noOfRequiredArgumentsAvailable = 0;
    int noOfRequiredArguments = 11;
    float[] frameLengths;
    float k2;
    Hashtable<Region, RegionResource> regionToRegionResourceHash;
    String renderingConfigFile = null;
    private static final String CEREBELLUM ="CEREBELLUM"; 
    static Logger logger = Logger.getLogger(PETTimeSeriesAssessorCreator.class);
    
    public PETTimeSeriesAssessorCreator(String args[]) {
        parseCommandLineArguments(args);
        regionToRegionResourceHash = new Hashtable<Region,RegionResource>();
        frameLengths = null;
    }
    
    private boolean isCerebellumAssessed() {
        boolean rtn = false;
        PetTimeCourseData petTimeCourseData = petTimeCourseDoc.getPETTimeCourse();
        if (petTimeCourseData.isSetRegions()) {
            for (int i = 0; i < petTimeCourseData.getRegions().sizeOfRegionArray(); i++) {
                Region region = petTimeCourseData.getRegions().getRegionArray(i);
                RegionResource petSessionRegionRsc = getRegionResource(region.getName(), region.getHemisphere().toString());
                if (petSessionRegionRsc.getName().equalsIgnoreCase(CEREBELLUM)) {
                    rtn = true;
                    break;
                }
            }
        }
        return rtn;
    }
    
    public void addTimeSeriesData() {
        try {
            setPetSessionFromHost();
            if (petSession != null) {
                setPETTimeSeriesFromHost();
                if (petTimeCourseDoc != null) {
                    parseTisListFile();
                    setFrameLengths();
                    plotAveragedRegions();
                    petTimeCourseDoc.save(new File("timeCourse.xml"), new XmlOptions().setSavePrettyPrint());
                    new XMLStore(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).store(petTimeCourseDoc.xmlText(new XmlOptions().setSavePrettyPrint().setSaveAggressiveNamespaces()));
                    System.out.println("Session stored");
                    logger.info("All done");
                }
            }
            
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private void parseCommandLineArguments(String args[]) {
        commandLineArgs = new Hashtable<String,String>();
        int c;
        LongOpt[] longopts = new LongOpt[15];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[1] = new LongOpt("sessionId", LongOpt.REQUIRED_ARGUMENT, null, 's');
        longopts[2] = new LongOpt("xnatId", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        longopts[3] = new LongOpt("tislist", LongOpt.REQUIRED_ARGUMENT, null, 't');
        longopts[4] = new LongOpt("u", LongOpt.REQUIRED_ARGUMENT, null, 'u');
        longopts[5] = new LongOpt("pwd", LongOpt.REQUIRED_ARGUMENT, null, 'w');
        longopts[6] = new LongOpt("buildDir", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        longopts[7] = new LongOpt("archiveDir", LongOpt.REQUIRED_ARGUMENT, null, 'a');
        longopts[8] = new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[9] = new LongOpt("log", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        longopts[10] = new LongOpt("totalframes", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        longopts[11] = new LongOpt("startframe", LongOpt.REQUIRED_ARGUMENT, null, 'm');
        //longopts[11] = new LongOpt("defaultstartframe", LongOpt.REQUIRED_ARGUMENT, null, 'e');
        longopts[12] = new LongOpt("k2", LongOpt.REQUIRED_ARGUMENT, null, 'k');
        longopts[13] = new LongOpt("config", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[14] = new LongOpt("project", LongOpt.REQUIRED_ARGUMENT, null, 'p');
        
        //Getopt g = new Getopt("PETTimeSeriesAssessorCreator", args, "s:t:u:w:d:a:o:l:f:m:e:k:h;", longopts, true);
        Getopt g = new Getopt("PETTimeSeriesAssessorCreator", args, "s:i:t:u:w:d:a:o:l:f:m:k:c:p:h;", longopts, true);
        g.setOpterr(false); // We'll do our own error handling
        //
        while ((c = g.getopt()) != -1) {
          switch (c)
            {
                case 'l':
                    commandLineArgs.put("log",g.getOptarg());
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
                    commandLineArgs.put("tisListFilePath",g.getOptarg());
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
                    commandLineArgs.put("sessionarchiveDir",adir);
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'f':
                    commandLineArgs.put("totalframes",g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'm':
                    commandLineArgs.put("startframe",g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                    // case 'e':
                //    commandLineArgs.put("defaultstartframe",g.getOptarg());
                //    break;
                case 'k':
                    k2 = Float.parseFloat(g.getOptarg());
                    noOfRequiredArgumentsAvailable++;
                    break;
                case 'c':
                    renderingConfigFile = g.getOptarg();
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
    
    private void setPetSessionFromHost() throws Exception {
        String createdFile = new XMLSearch(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password")).searchFirst("xnat:petSessionData.ID",commandLineArgs.get("xnatId"), "=","xnat:petSessionData",FileUtils.getTempFolder());
        petSession = (PETSessionDocument)new XmlReader().read(createdFile, true);
   }
    
    private String getPathToProjectSpecificAssessor() throws Exception{
    	String project = commandLineArgs.get("project");
        XMLSearch xmlSearch = new XMLSearch(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password"));
        ArrayList<String> createdFiles = xmlSearch.searchAll("cnda:petTimeCourseData.imageSession_ID",petSession.getPETSession().getID(), "=","cnda:petTimeCourseData",FileUtils.getTempFolder());
        String createdFile = null;
        for (String path : createdFiles) {
	        	 PETTimeCourseDocument temp  = (PETTimeCourseDocument)new XmlReader().read(path, true);
	        	 if (temp.getPETTimeCourse().getProject().equals(project)) {
	            	 petTimeCourseDoc = temp;
	            	 createdFile = path;
	        	 }
        }
        return createdFile;
    }
    
    private void setPETTimeSeriesFromHost() throws Exception {
        XMLSearch xmlSearch = new XMLSearch(commandLineArgs.get("host"), commandLineArgs.get("username"), commandLineArgs.get("password"));
        petTimeCourseDoc = null;
        ArrayList<String> timeCourses = new ArrayList<String>();
        timeCourses = xmlSearch.searchAll("cnda:petTimeCourseData.imageSession_ID",petSession.getPETSession().getID(), "=","cnda:petTimeCourseData",FileUtils.getTempFolder());
        if (timeCourses.size() > 0) {
        	for (int i = 0; i < timeCourses.size(); i++) {
        		PETTimeCourseDocument petTimeCourseDocTemp = (PETTimeCourseDocument)new XmlReader().read(timeCourses.get(i), true);
        		if (petTimeCourseDocTemp.getPETTimeCourse().getID().startsWith(petSession.getPETSession().getID() + "_ROI_TIMECOURSE")) {
        			petTimeCourseDoc = petTimeCourseDocTemp;
        			break;
        		}
        	}
        }else {
            petTimeCourseDoc = PETTimeCourseDocument.Factory.newInstance();
            PetTimeCourseData petTimeCourseData = petTimeCourseDoc.addNewPETTimeCourse();
            petTimeCourseData.setImageSessionID(petSession.getPETSession().getID());
            petTimeCourseData.setProject(petSession.getPETSession().getProject());
            Calendar calendar = Calendar.getInstance();
            Date date = calendar.getTime();
            
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            DateFormat dateFormat1 = new SimpleDateFormat("HH:mm:ss");

            String idSuffix = dateFormat.format(date);
            XmlCalendar xmlCalendar = new XmlCalendar(idSuffix);
            petTimeCourseData.setID(petSession.getPETSession().getID() + "_ROI_TIMECOURSE" + "_" + idSuffix);

            petTimeCourseData.setDate(xmlCalendar);
            petTimeCourseData.setTime(new XmlCalendar(dateFormat1.format(date)));
            if (petSession.getPETSession().isSetInvestigator())
            	petTimeCourseData.setInvestigator(petSession.getPETSession().getInvestigator());
        }
   }

       
   private Region getRegion(String regionName, String regionHemisphere) {
       PetTimeCourseData petTimeCourse = petTimeCourseDoc.getPETTimeCourse();
       Region newRgn = null;
       if (petTimeCourse.isSetRegions()) {
           Region[] regions = petTimeCourse.getRegions().getRegionArray();
           for (int i = 0; i < regions.length; i++) {
               if (regions[i].getName().equals(regionName) && regions[i].getHemisphere().toString().equals(regionHemisphere)) {
                   petTimeCourse.getRegions().removeRegion(i); //Recomputation for the region
                   break;
               }
           }
       }
       if (newRgn == null && !petTimeCourse.isSetRegions()) {
           newRgn = petTimeCourse.addNewRegions().addNewRegion();
       }else if (newRgn == null ) {
           newRgn = petTimeCourse.getRegions().addNewRegion();
       }
       newRgn.setName(regionName); newRgn.setHemisphere(PetTimeCourseData.Regions.Region.Hemisphere.Enum.forString(regionHemisphere));
       newRgn.addNewTimeSeries();
       return newRgn;
   }
    
    private Hashtable getRegionIdWithLabelsFromPetSession(RegionResource regionResource) {
        Hashtable<Integer,Label> rtn = null;
        if (regionResource != null) {
            rtn = new Hashtable<Integer,Label>();
            if (regionResource.isSetSubregionlabels()){
                Label[] labels = regionResource.getSubregionlabels().getLabelArray();
                for (int j = 0; j < labels.length; j++) {
                   // System.out.println("Lable " + labels[j].toString());
                    rtn.put(new Integer(labels[j].getId()),labels[j]);
                }
            }
        }
        return rtn;
    }
    
    private RegionResource getRegionResource(String objFileRoot) {
        RegionResource regionResource = null;
        if (petSession != null) {
            PetSessionData petSessionData = petSession.getPETSession();
            if (petSessionData.isSetRegions()) {
                if (!objFileRoot.endsWith(".obj")) {
                    objFileRoot += ".obj";
                }
                RegionResource[] regionRscs = petSessionData.getRegions().getRegionArray();
                for (int i = 0; i < regionRscs.length; i++) {
                    AbstractResource objMap = regionRscs[i].getFile();
                    if (objMap instanceof ImageResource) {
                        ImageResource imgRsc = (ImageResource)regionRscs[i].getFile();
                        if (imgRsc.getURI().endsWith(objFileRoot)) {
                            regionResource = regionRscs[i];
                            break;
                        }
                    }
                    
                }
            }
        }
        return regionResource;
    }
    
    private RegionResource getRegionResource(String subRegionName, String subRegionHemisphere) {
        RegionResource regionResource = null;
        if (petSession != null) {
            PetSessionData petSessionData = petSession.getPETSession();
            if (petSessionData.isSetRegions()) {
                RegionResource[] regionRscs = petSessionData.getRegions().getRegionArray();
                for (int i = 0; i < regionRscs.length; i++) {
                       Label[] labels = regionRscs[i].getSubregionlabels().getLabelArray();
                       for (int j =0; j < labels.length; j++) {
                            if (labels[j].getStringValue().equals(subRegionName) && labels[j].getHemisphere().toString().equals(subRegionHemisphere) ) {
                                regionResource = regionRscs[i];
                                break;
                            }
                       }
                    }
                }
            }
        return regionResource;
    }

    
    private void parseTisListFile() {
        String tisListFilePath = commandLineArgs.get("tisListFilePath");
        PetTimeCourseData petTimeCourse = petTimeCourseDoc.getPETTimeCourse();
        String relativePath = FileUtils.getRelativePath(petSession);
        String sessionId = commandLineArgs.get("sessionId");
        try {
            BufferedReader in = new BufferedReader(new FileReader(tisListFilePath));
            String str;
            Hashtable<RegionResource, ArrayList<Region>> regionRscRegions = new Hashtable<RegionResource,ArrayList<Region>>();
            while ((str = in.readLine()) != null) {
                String[] tokens = StringUtils.split(str);
                if (tokens != null) {
                    String objFileRoot = tokens[0];
                    String regionid = tokens[1];
                    String tisFilePath = tokens[2];
                    RegionResource regionRsc = getRegionResource(objFileRoot);
                    Hashtable<Integer,Label> regionLabels = getRegionIdWithLabelsFromPetSession(regionRsc);
                    if (regionLabels != null) {
                        Label label = regionLabels.get(new Integer(Integer.parseInt(regionid)));
                        Region region = getRegion(label.getStringValue(), label.getHemisphere().toString());
                        insertRegionTimeSeriesData(region,tisFilePath);
                        if (regionRscRegions.containsKey(regionRsc)) {
                            regionRscRegions.get(regionRsc).add(region);
                        }else {
                            ArrayList<Region> regionList = new ArrayList<Region>();
                            regionList.add(region);
                            regionRscRegions.put(regionRsc,regionList);
                        }
                        Out files = null;
                        boolean check = false;
                        if (petTimeCourse.isSetOut()) {
                            files = petTimeCourse.getOut();
                            check = true;
                        }else
                            files = petTimeCourse.addNewOut();
                        File file = new File(tisFilePath);
                        String uriPath = file.getPath().replaceAll("\\\\","/");
                        String fileUri = FileUtils.getPath(uriPath,relativePath, sessionId);
                        if (check) removePreExistingFile(fileUri);
                        AbstractResource absRsc = files.addNewFile();
                        Resource rsc = (Resource)absRsc.changeType(Resource.type);
                        rsc.setURI(fileUri);
                        rsc.setFormat("TXT");
                        rsc.setContent("TIMESERIES");
                    }else {
                        throw new Exception("A region label wasnt found for " + objFileRoot + " " + regionid);
                    }
                }
            }
            in.close();
            computeBPAndCreateLoganPlots(regionRscRegions);
        } catch (Exception e) {
            System.out.println("Encountered Error! See log file for details");
            logger.error("Couldnt set regions " + tisListFilePath,e);
            System.exit(1);
        }
    }

    
    private void removePreExistingFile(String uri) {
        for (int i = 0; i < petTimeCourseDoc.getPETTimeCourse().getOut().sizeOfFileArray(); i++) {
            AbstractResource absRsc = petTimeCourseDoc.getPETTimeCourse().getOut().getFileArray(i);
            try {
                Resource rsc = (Resource)absRsc.changeType(Resource.type);
                if (rsc.getURI().equals(uri)) {
                    petTimeCourseDoc.getPETTimeCourse().getOut().removeFile(i);
                }
            }catch(Exception e) {}
        }
    }
    
    private boolean isCerebellum(RegionResource regionRsc) {
        boolean cerebellum = true;
        if (regionRsc.getName().equalsIgnoreCase("CEREBELLUM")) {
            return cerebellum;
        }
        return !cerebellum;
    }
    
    
    
    
    
    private void insertRegionTimeSeriesData(Region region, String pathToTisFile) {
        try {
            BufferedReader in = new BufferedReader(new FileReader(pathToTisFile));
            String str;
            TimeSeries timeSeries = region.getTimeSeries();
            while ((str = in.readLine()) != null) {
                String[] tokens = StringUtils.split(str);
                if (tokens != null && tokens.length == 3) {
                   Activity activity = timeSeries.addNewActivity();
                    activity.setTime(Float.parseFloat(tokens[0]));
                    activity.setFloatValue(Float.parseFloat(tokens[1]));
                }
            }
            in.close();
            //logger.debug("TIS FILE " + pathToTisFile + " " + petTimeCourseDoc.getPETTimeCourse().getRegions().sizeOfRegionArray());
        }catch(FileNotFoundException fne) {
            logger.error("Couldnt read " + pathToTisFile,fne);
            System.exit(1);
        }catch(IOException ioe) {
            logger.error("Couldnt read " + pathToTisFile,ioe);
            System.exit(1);
        }
        
        
    }
    
    private void plotAveragedRegions() {
        Hashtable<RegionResource,Region> avgOverRegion = averageOverRegion(groupByRegion());
        ArrayList<Region> fusedFrames = fuseFrames(avgOverRegion);
        XYSeriesCollection dataSet = new XYSeriesCollection();
        for (int i = 0; i < fusedFrames.size(); i++ ) {
            Region region = fusedFrames.get(i);
            XYSeries series = new XYSeries(region.getName() );
            TimeSeries timeSeries = region.getTimeSeries();
            for (int j = 0; j < timeSeries.sizeOfActivityArray(); j++) {
              series.add(timeSeries.getActivityArray(j).getTime(), timeSeries.getActivityArray(j).getFloatValue());
            }
            dataSet.addSeries(series);
        }
            JFreeChart chart = ChartFactory.createXYLineChart
                                 (petSession.getPETSession().getLabel(),  // Title
                                  "Time(min)",           // X-Axis label
                                  "Activity",           // Y-Axis label
                                  dataSet,          // Dataset
                                  PlotOrientation.VERTICAL,
                                  true,                // Show legend
                                  true,
                                  false
                                 );
            chart.setBorderPaint(Color.WHITE);
            chart.getLegend().setBackgroundPaint(new Color(200, 200, 200));
            if (renderingConfigFile != null) {
                Properties properties = new Properties();
                try {
                        properties.load(new FileInputStream(renderingConfigFile));
                        XYPlot plot = (XYPlot) chart.getPlot();
                        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer)(plot).getRenderer();
                        plot.setRenderer(renderer);
                        plot.setBackgroundPaint(new Color(200, 200, 200));
                        for (int i = 0; i < fusedFrames.size(); i++ ) {
                            Region region = fusedFrames.get(i);
                            renderer.setSeriesPaint(i, getColor(properties.getProperty(region.getName().toUpperCase().replaceAll(" ",""))));
                        }
                }catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            try {
                File qcDir = new File(commandLineArgs.get("sessionarchiveDir") + File.separator + commandLineArgs.get("sessionId") + File.separator + "QC");
                if (!qcDir.exists()) {
                    qcDir.mkdir();
                }
                Out files = null;
                boolean check = false;
                if (petTimeCourseDoc.getPETTimeCourse().isSetOut()) {
                    files = petTimeCourseDoc.getPETTimeCourse().getOut();
                    check = true;
                }else
                    files = petTimeCourseDoc.getPETTimeCourse().addNewOut();
                String relativePath = FileUtils.getRelativePath(petSession);
                String sessionId = commandLineArgs.get("sessionId");
                String fileName = qcDir.getAbsolutePath() + File.separator + petSession.getPETSession().getLabel() + "_"+ "roi_activity_plot.jpg";
                File plotFile = new File(fileName);
                ChartUtilities.saveChartAsJPEG(plotFile, chart, 500, 300);
                String uriPath = plotFile.getPath().replaceAll("\\\\","/");
                String fileUri = FileUtils.getPath(uriPath,relativePath, sessionId);
                AbstractResource absRsc = AbstractResource.Factory.newInstance();
                Resource rsc = (Resource)absRsc.changeType(Resource.type);
                rsc.setURI(fileUri);
                rsc.setFormat("JPEG");
                rsc.setContent("PLOT");
                if (check) {
                    check = removeResource(files,rsc);
                    System.out.println("Found the file " + check);
                }
               AbstractResource absRsc1 = files.addNewFile();
               absRsc1.set(rsc);
             } catch (IOException e) {
                    System.err.println("Problem occurred creating chart.");
             }
    }
    
    private Color getColor(String colonSeparatedRGB) {
        String[] rgb = colonSeparatedRGB.split(":");
        return new Color(Integer.parseInt(rgb[0]),Integer.parseInt(rgb[1]),Integer.parseInt(rgb[2]) );
    }
    

    
    private void computeBPAndCreateLoganPlots(Hashtable<RegionResource, ArrayList<Region>> regionRscRegions) {
        Enumeration<RegionResource> keys = regionRscRegions.keys();
        int startFrame  = Integer.parseInt(commandLineArgs.get("startframe"));
        while (keys.hasMoreElements()) {
            RegionResource regionRsc = keys.nextElement();
            if (regionRsc.getName().equalsIgnoreCase(CEREBELLUM))
               break;
            if (isCerebellumAssessed()) {
                Hashtable<RegionResource, ArrayList<Region>> grpdByRegionResource = groupByRegion();
                Hashtable<RegionResource,Region> avgOverRegion = averageOverRegion(regionRscRegions);
                RegionResource cerebellumRsc = getCerebellumRegionResource();
                if (cerebellumRsc == null)
                    break;
                Hashtable<RegionResource,ArrayList<Region>> cerebellumRegions = new Hashtable<RegionResource,ArrayList<Region>>();
                cerebellumRegions.put(cerebellumRsc,grpdByRegionResource.get(cerebellumRsc));
                Hashtable<RegionResource,Region> avgOverCerebellum = averageOverRegion(cerebellumRegions);
                Region tavgOverRegionRgn = avgOverRegion.get(regionRsc);
                Region tavgOverCerebellumRgn = avgOverCerebellum.get(cerebellumRsc);
                Region avgOverRegionRgn = prefixWithZeroFrames(tavgOverRegionRgn);
                Region avgOverCerebellumRgn = prefixWithZeroFrames(tavgOverCerebellumRgn);     
                Region integralOverCerebellum = computeIntegralOverRegion(avgOverCerebellumRgn);
                Region integralOverRegion = computeIntegralOverRegion(avgOverRegionRgn);
                TimeSeries integralOverCerebellumTS = integralOverCerebellum.getTimeSeries();
                TimeSeries averageOverCerebellumTS = avgOverCerebellumRgn.getTimeSeries();
                TimeSeries avgOverRegionTS = avgOverRegionRgn.getTimeSeries();
                Region computationBlock1 = Region.Factory.newInstance();
                TimeSeries computationBlock1TS = computationBlock1.addNewTimeSeries();
                for (int i = startFrame - 1; i < frameLengths.length; i++) {
                    Activity icActivity = integralOverCerebellumTS.getActivityArray(i);
                    Activity avgCerebellumActivity = averageOverCerebellumTS.getActivityArray(i);
                    Activity avgRegionActivity = avgOverRegionTS.getActivityArray(i);
                    Activity compBlock1Activity = computationBlock1TS.addNewActivity();
                    compBlock1Activity.setTime(avgRegionActivity.getTime());
                    float f = (icActivity.getFloatValue() + avgCerebellumActivity.getFloatValue()/k2)/avgRegionActivity.getFloatValue(); 
                    compBlock1Activity.setFloatValue(f);
                }
                Region computationBlock2 = Region.Factory.newInstance();
                TimeSeries integralOverRegionTS = integralOverRegion.getTimeSeries(); 
                TimeSeries computationBlock2TS = computationBlock2.addNewTimeSeries();
                for (int i = startFrame - 1; i < frameLengths.length; i++) {
                    Activity irActivity = integralOverRegionTS.getActivityArray(i);
                    Activity avgRegionActivity = avgOverRegionTS.getActivityArray(i);
                    Activity compBlock2Activity = computationBlock2TS.addNewActivity();
                    compBlock2Activity.setTime(irActivity.getTime());
                    compBlock2Activity.setFloatValue(irActivity.getFloatValue()/avgRegionActivity.getFloatValue());
                }
                Region tcb1 = prefixWithZeroFrames(computationBlock1);
                Region tcb2 = prefixWithZeroFrames(computationBlock2);
                setDurations(regionRsc,tcb1, tcb2);
            }
        }
    }
    
/*    private void setDurations(RegionResource regionRsc,Region computationBlock1, Region computationBlock2) {
        //int startFrame  = Integer.parseInt(commandLineArgs.get("startframe"));
        PetTimeCourseData petTimeCourse = petTimeCourseDoc.getPETTimeCourse();
        Durations durations = null;
        Duration d20_60 = null;
        Duration d30_60 = null;
        String str2060 = "20-60 Minutes";
        String str3060 = "30-60 Minutes";
        String bpName = regionRsc.getName() ;
        if (petTimeCourse.isSetDurations()) {
            durations = petTimeCourse.getDurations();
            for (int i = 0; i < durations.sizeOfDurationArray(); i++) {
              if (durations.getDurationArray(i).getSpan().equals(str2060)) {
                  d20_60 = durations.getDurationArray(i);
              }
              if (durations.getDurationArray(i).getSpan().equals(str3060)) {
                  d30_60 = durations.getDurationArray(i);
              }
            }
            if (d20_60 == null) {
                d20_60 = durations.addNewDuration();
                d20_60.setSpan(str2060);
            }
            if (d30_60 == null) {
                d30_60 = durations.addNewDuration();
                d30_60.setSpan(str3060);
            }
        }else {
            durations = petTimeCourse.addNewDurations();
            d20_60 = durations.addNewDuration();
            d20_60.setSpan(str2060);
            d30_60 = durations.addNewDuration();
            d30_60.setSpan(str3060);
        }
        Bp bp20_60 = null; Bp bp30_60 = null;
        for (int i = 0; i < d20_60.sizeOfBpArray(); i++) {
            if (d20_60.getBpArray(i).getName().equals(bpName)) {
                bp20_60 = d20_60.getBpArray(i);
            }
        }
        for (int i = 0; i < d30_60.sizeOfBpArray(); i++) {
            if (d30_60.getBpArray(i).getName().equals(bpName)) {
                bp30_60 = d30_60.getBpArray(i);
            }
        }
        if (bp20_60 == null) {
            bp20_60 = d20_60.addNewBp();
        }
        if (bp30_60 == null) {
            bp30_60 = d30_60.addNewBp();
        }
        bp20_60.setName(bpName); bp30_60.setName(bpName);
        //20-60 Minutes
        try {
            double x[] = getDoubleArrayOfActivityValue(computationBlock1.getTimeSeries(),46,54);
            double y[] = getDoubleArrayOfActivityValue(computationBlock2.getTimeSeries(),46,54);
            double slope = Statistics.slopeByLeastSquareBestLineFit(x,y);
            double correl = Statistics.linearCorrelation(getDoubleArrayOfActivityValue(computationBlock2.getTimeSeries(),46,54),getDoubleArrayOfActivityValue(computationBlock1.getTimeSeries(),46,54));
            bp20_60.setFloatValue((float)(slope-1));
            bp20_60.setCorrel((float)correl);
            plotLogan(x,y, StringUtils.deleteWhitespace(regionRsc.getName()) +"_20_60     R2 = "+org.nrg.pipeline.client.utils.StringUtils.d2s(correl*correl,4));
            x = getDoubleArrayOfActivityValue(computationBlock1.getTimeSeries(),48,54);
            y = getDoubleArrayOfActivityValue(computationBlock2.getTimeSeries(),48,54);
            slope = Statistics.slopeByLeastSquareBestLineFit(x,y);
            correl = Statistics.linearCorrelation(getDoubleArrayOfActivityValue(computationBlock2.getTimeSeries(),48,54),getDoubleArrayOfActivityValue(computationBlock1.getTimeSeries(),48,54));
            bp30_60.setFloatValue((float)(slope-1));
            bp30_60.setCorrel((float)correl);
            plotLogan(x,y, StringUtils.deleteWhitespace(regionRsc.getName())+"_30_60      R2 = "+org.nrg.pipeline.client.utils.StringUtils.d2s(correl*correl,4));
        }catch(Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(-1);
        }
    } */
    
    private void setDurations(RegionResource regionRsc,Region computationBlock1, Region computationBlock2) {
        //int startFrame  = Integer.parseInt(commandLineArgs.get("startframe"));
    	//MR: May 28, 2009 - 20-60 frames are no longer needed
        PetTimeCourseData petTimeCourse = petTimeCourseDoc.getPETTimeCourse();
        Durations durations = null;
        Duration d30_60 = null;
        String str3060 = "30-60 Minutes";
        String bpName = regionRsc.getName() ;
        if (petTimeCourse.isSetDurations()) {
            durations = petTimeCourse.getDurations();
            for (int i = 0; i < durations.sizeOfDurationArray(); i++) {
              if (durations.getDurationArray(i).getSpan().equals(str3060)) {
                  d30_60 = durations.getDurationArray(i);
              }
            }
            if (d30_60 == null) {
                d30_60 = durations.addNewDuration();
                d30_60.setSpan(str3060);
            }
        }else {
            durations = petTimeCourse.addNewDurations();
            d30_60 = durations.addNewDuration();
            d30_60.setSpan(str3060);
        }
        Bp bp30_60 = null;
        for (int i = 0; i < d30_60.sizeOfBpArray(); i++) {
            if (d30_60.getBpArray(i).getName().equals(bpName)) {
                bp30_60 = d30_60.getBpArray(i);
            }
        }
        if (bp30_60 == null) {
            bp30_60 = d30_60.addNewBp();
        }
        bp30_60.setName(bpName); 
        //30-60 Minutes
        try {
            double x[] = getDoubleArrayOfActivityValue(computationBlock1.getTimeSeries());
            double y[] = getDoubleArrayOfActivityValue(computationBlock2.getTimeSeries());
            double slope = Statistics.slopeByLeastSquareBestLineFit(x,y);
            double correl = Statistics.linearCorrelation(getDoubleArrayOfActivityValue(computationBlock2.getTimeSeries()),getDoubleArrayOfActivityValue(computationBlock1.getTimeSeries()));
            bp30_60.setFloatValue((float)(slope-1));
            bp30_60.setCorrel((float)correl);
            plotLogan(x,y, StringUtils.deleteWhitespace(regionRsc.getName()) +"_30_60     R2 = "+org.nrg.pipeline.client.utils.StringUtils.d2s(correl*correl,4));
        }catch(Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(-1);
        }
    }
    
    private void plotLogan(double x[], double y[], String title) {
        XYSeriesCollection dataSet = new XYSeriesCollection();
        XYSeries series = new XYSeries(title);
        for (int i = 0; i < x.length; i++ ) {
              series.add(x[i],y[i]);
        }
        dataSet.addSeries(series);
        
        XYSeries seriesBestFit = new XYSeries("Best Fit");
        try {
            Statistics.setLeastSquareBestLine(x,y,seriesBestFit);
            dataSet.addSeries(seriesBestFit);
            
            JFreeChart chart = ChartFactory.createXYLineChart
                                 ("",  // Title
                                  "",           // X-Axis label
                                  "",           // Y-Axis label
                                  dataSet,          // Dataset
                                  PlotOrientation.VERTICAL,
                                  true,                // Show legend
                                  true,
                                  false
                                 );
            chart.setBorderPaint(Color.white);
            //chart.addSubtitle(new TextTitle(rSqr));
            final XYPlot plot = chart.getXYPlot();
            
            final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
            renderer.setSeriesLinesVisible(0, false);
            renderer.setSeriesShapesVisible(1,false);
            plot.setRenderer(renderer);

                File qcDir = new File(commandLineArgs.get("sessionarchiveDir") + File.separator + commandLineArgs.get("sessionId") + File.separator + "QC");
                if (!qcDir.exists()) {
                    qcDir.mkdir();
                }
                Out files = null;
                boolean check = false;
                if (petTimeCourseDoc.getPETTimeCourse().isSetOut()) {
                    files = petTimeCourseDoc.getPETTimeCourse().getOut();
                    check = true;
                }else
                    files = petTimeCourseDoc.getPETTimeCourse().addNewOut();
                String relativePath = FileUtils.getRelativePath(petSession);
                String sessionId = commandLineArgs.get("sessionId");
                String parts[] = StringUtils.split(title);
                String fileName = qcDir.getAbsolutePath() + File.separator + petSession.getPETSession().getLabel() + "_" + parts[0] +  ".jpg";
                File plotFile = new File(fileName);
                ChartUtilities.saveChartAsJPEG(plotFile, chart, 300, 200);
                String uriPath = plotFile.getPath().replaceAll("\\\\","/");
                String fileUri = FileUtils.getPath(uriPath,relativePath, sessionId);
                AbstractResource absRsc = AbstractResource.Factory.newInstance();
                Resource rsc = (Resource)absRsc.changeType(Resource.type);
                rsc.setURI(fileUri);
                rsc.setFormat("JPEG");
                rsc.setContent("LOGANPLOT");
                if (check) {
                  check = removeResource(files,rsc);
                }
                AbstractResource absRsc1 = files.addNewFile();
                absRsc1.set(rsc);
             } catch (Exception e) {
                     e.printStackTrace();
                    System.err.println("Problem occurred while creating Logan chart. " + e.getMessage());
             }
    }
    
    private boolean removeResource(Out files, Resource absRsc) {
        boolean removed = false;
        if (files == null) return !removed;
        if (absRsc == null) return !removed;
        for (int i = 0; i < files.sizeOfFileArray(); i++) {
            Resource rsc = (Resource)files.getFileArray(i).changeType(Resource.type);
            if (rsc.getURI().equals(absRsc.getURI()) && rsc.getFormat().equals(absRsc.getFormat()) && rsc.getContent().equals(absRsc.getContent())) {
                files.removeFile(i);
                removed = true;
                break;
            }
        }
        return removed;
    }
    
    
    private Region computeIntegralOverRegion(Region averageOverRegion) {
        Region integralOverRegion = Region.Factory.newInstance();
        if (frameLengths == null) setFrameLengths();
        TimeSeries timeSeries = averageOverRegion.getTimeSeries();
        TimeSeries integralOverRegionTimeSeries = integralOverRegion.addNewTimeSeries();
        Activity zerothActivity = integralOverRegionTimeSeries.addNewActivity();
        zerothActivity.setTime(0); zerothActivity.setFloatValue(0);
        for (int i = 1; i < timeSeries.sizeOfActivityArray(); i++ ) {
            Activity activity = integralOverRegionTimeSeries.addNewActivity();
            activity.setTime(timeSeries.getActivityArray(i).getTime()); 
            activity.setFloatValue(integralOverRegionTimeSeries.getActivityArray(i-1).getFloatValue() + frameLengths[i]*timeSeries.getActivityArray(i).getFloatValue());
        }
        
        return integralOverRegion;
    }
    
    private RegionResource getCerebellumRegionResource() {
        RegionResource rtn = null;
        PetSessionData petSessionData = petSession.getPETSession();
        if (petSessionData.isSetRegions()) {
            for (int i = 0; i < petSessionData.getRegions().sizeOfRegionArray(); i++) {
                RegionResource region = petSessionData.getRegions().getRegionArray(i);
                if (isCerebellum(region)) {
                    rtn = region;
                    break;
                }
            }
        }
        return rtn;
    }
    
    
    
    
    
    private void setFrameLengths() {
        /*
         * This is column U of the PIB Excel sheet
         */
        int totalFrame = Integer.parseInt(commandLineArgs.get("totalframes"));
        PetTimeCourseData petTimeCourse = petTimeCourseDoc.getPETTimeCourse();
        if (petTimeCourse.isSetRegions() && frameLengths == null) {
            Region region = petTimeCourse.getRegions().getRegionArray(0);
            frameLengths = new float[totalFrame];
           for (int i = 0; i < frameLengths.length; i++) frameLengths[i] = 0;
            int diff = totalFrame - region.getTimeSeries().sizeOfActivityArray();
            for (int i = diff; i < totalFrame; i++) {
                Activity activity = region.getTimeSeries().getActivityArray(i-diff);
                frameLengths[i] = 2* (activity.getTime() - sum(frameLengths,0,i));
            }
            //for (int i = 0; i < 10; i++) frameLengths[i] = 0;
            //for (int i = 10; i < 26; i++) frameLengths[i] = (float)0.0833;
            //for (int i = 26; i < 35; i++) frameLengths[i] = (float)0.3333;
            //for (int i = 35; i < 45; i++) frameLengths[i] = 1;
            //for (int i = 45; i < 54; i++) frameLengths[i] = 5;
        }

    }
    
    private void printToFile(Region region, String fileName) {
        try {
            FileWriter outFile = new FileWriter(fileName);
            PrintWriter out = new PrintWriter(outFile);
            // Write text to file
            for (int i = 0; i < region.getTimeSeries().sizeOfActivityArray(); i++) {
                out.println(i + "=" + region.getTimeSeries().getActivityArray(i).getFloatValue());
            }
            out.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    
    private float sum(float[] fArray, int startIndex, int endIndex) {
        float rtn = 0;
        for (int i = startIndex; i < endIndex; i++) {
            rtn += fArray[i];
        }
        return rtn;
    }
    
    private double[] getDoubleArrayOfActivityValue(TimeSeries ts, int startIndex, int endIndex) {
        double[] rtn = null;
        int j = 0;
        rtn = new double[endIndex - startIndex];
        for (int i = startIndex; i < endIndex; i++, j++) {
            rtn[j] = ts.getActivityArray(i).getFloatValue();
        }
        return rtn;
    }
    
    private double[] getDoubleArrayOfActivityValue(TimeSeries ts ) {
        double[] rtn = null;
        int j = 0;
        int startIndex=0, endIndex=ts.getActivityArray().length;
        for (int i = 0; i < ts.getActivityArray().length; i++) {
        	if (ts.getActivityArray(i).getTime() >= 30) {
        		startIndex = i;
        		break;
        	}
        }
        rtn = new double[endIndex - startIndex];
        for (int i = startIndex; i < endIndex; i++, j++) {
            rtn[j] = ts.getActivityArray(i).getFloatValue();
        }
        return rtn;
    }
    
   /* private ArrayList<Region> fuseFrames(Hashtable<RegionResource,Region> avgOverRegion) {
        ArrayList<Region> fusedRegions = new ArrayList<Region>();
        Enumeration<RegionResource> keysEnum = avgOverRegion.keys();
        int totalFrame = Integer.parseInt(commandLineArgs.get("totalframes"));
        while (keysEnum.hasMoreElements()) {
            RegionResource regionRsc = keysEnum.nextElement();
            Region avgRegion = avgOverRegion.get(regionRsc);
            Region fusedRegion = Region.Factory.newInstance();
            Region tempRegion = prefixWithZeroFrames(avgRegion);
            fusedRegion.setName(avgRegion.getName());
            fusedRegion.setHemisphere(avgRegion.getHemisphere());
            TimeSeries fusedTimeSeries = fusedRegion.addNewTimeSeries();
            //0th time point is AVG(4 frame, 14 frame)
            Activity activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,3,14));
            activity.setFloatValue(getAverageActivity(tempRegion,3,14));
            //1st time point is AVG(14 frame, 26 frame)
            activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,14,26));
            activity.setFloatValue(getAverageActivity(tempRegion,14,26));
            //2nd time point is AVG(26 frame, 29 frame)
            activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,26,29));
            activity.setFloatValue(getAverageActivity(tempRegion,26,29));
            //3rd time point is AVG(29 frame, 32 frame)
            activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,29,32));
            activity.setFloatValue(getAverageActivity(tempRegion,29,32));
            //4th time point is AVG(32 frame, 35 frame)
            activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,32,35));
            activity.setFloatValue(getAverageActivity(tempRegion,32,35));
            for (int i = 5; i < 24; i++) {
                activity = fusedTimeSeries.addNewActivity();
                activity.set(tempRegion.getTimeSeries().getActivityArray(i+30));
            }
            
            fusedRegions.add(fusedRegion);
        }
        return fusedRegions;
    }*/
    
    private ArrayList<Region> fuseFrames(Hashtable<RegionResource,Region> avgOverRegion) {
      //MR - May 27, 2009
      //Due to introduction of new frames, only the first two rows will be fused.	
    	ArrayList<Region> fusedRegions = new ArrayList<Region>();
        Enumeration<RegionResource> keysEnum = avgOverRegion.keys();
        int totalFrame = Integer.parseInt(commandLineArgs.get("totalframes"));
        while (keysEnum.hasMoreElements()) {
            RegionResource regionRsc = keysEnum.nextElement();
            Region avgRegion = avgOverRegion.get(regionRsc);
            Region fusedRegion = Region.Factory.newInstance();
            Region tempRegion = prefixWithZeroFrames(avgRegion);
            TimeSeries oldTS = avgRegion.getTimeSeries();
            int diff = totalFrame - oldTS.sizeOfActivityArray();

            fusedRegion.setName(avgRegion.getName());
            fusedRegion.setHemisphere(avgRegion.getHemisphere());
            TimeSeries fusedTimeSeries = fusedRegion.addNewTimeSeries();
            //0th time point is AVG(diff frame, 5 frame)
            Activity activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,diff,diff+2));
            activity.setFloatValue(getAverageActivity(tempRegion,diff,diff+2));
            //1st time point is AVG(5 frame, 7 frame)
            activity = fusedTimeSeries.addNewActivity();
            activity.setTime(getAverageTimePoint(tempRegion,diff+2,diff+4));
            activity.setFloatValue(getAverageActivity(tempRegion,diff+2,diff+4));
            for (int i = 2; i < totalFrame - diff; i++) {
                activity = fusedTimeSeries.addNewActivity();
                activity.set(tempRegion.getTimeSeries().getActivityArray(i - 2));
            }
            
            fusedRegions.add(fusedRegion);
        }
        return fusedRegions;
    }
    
    private Region prefixWithZeroFrames(Region region) {
        int totalFrame = Integer.parseInt(commandLineArgs.get("totalframes"));
        Region tempRegion = Region.Factory.newInstance();
        tempRegion.set(region);
        TimeSeries oldTS = tempRegion.getTimeSeries();
        if (oldTS.sizeOfActivityArray() < totalFrame) {
            TimeSeries tempNewTS = TimeSeries.Factory.newInstance();
            //Prepend 0 from the 4th frame onwards
            for (int i = 0; i < totalFrame - oldTS.sizeOfActivityArray(); i++) {
                Activity tempActivity = tempNewTS.addNewActivity();
                tempActivity.setTime(0); tempActivity.setFloatValue(0);
            }
            int diff = totalFrame - oldTS.sizeOfActivityArray();
            for (int i = diff; i < totalFrame; i++) {
                Activity tempActivity = tempNewTS.addNewActivity();
                tempActivity.setTime(oldTS.getActivityArray(i-diff).getTime());
                tempActivity.setFloatValue(oldTS.getActivityArray(i-diff).getFloatValue());
            }
            tempRegion.setTimeSeries(tempNewTS);
        }
        return tempRegion;
    }
    
    private float getAverageActivity(Region region, int startTimeIndex, int endTimeIndex) {
        float rtn = 0;
        for (int i = startTimeIndex; i < endTimeIndex; i++ ) {
            rtn += region.getTimeSeries().getActivityArray(i).getFloatValue();
        }
        rtn = rtn /(endTimeIndex - startTimeIndex);
        return rtn;
    }
    
    private float getAverageTimePoint(Region region, int startTimeIndex, int endTimeIndex) {
        float rtn = 0;
        for (int i = startTimeIndex; i < endTimeIndex; i++ ) {
            rtn += region.getTimeSeries().getActivityArray(i).getTime();
        }
        rtn = rtn /(endTimeIndex - startTimeIndex);
        return rtn;
    }
    
    private Hashtable<RegionResource,Region> averageOverRegion(Hashtable<RegionResource, ArrayList<Region>> grpdRegionResources) {
        Hashtable<RegionResource,Region> rtn = new Hashtable<RegionResource,Region>();
        Enumeration<RegionResource> keysEnum = grpdRegionResources.keys();
        while (keysEnum.hasMoreElements()) {
            RegionResource regionRsc = keysEnum.nextElement();
            Region avgRegion = Region.Factory.newInstance();
            //avgRegion.setName("AVG_" + regionRsc.getName());
            avgRegion.setName(regionRsc.getName());
            avgRegion.setHemisphere(PetTimeCourseData.Regions.Region.Hemisphere.Enum.forString(regionRsc.getHemisphere().toString()));
            TimeSeries avgTimeSeries = avgRegion.addNewTimeSeries();
            ArrayList<Region> subRegions = grpdRegionResources.get(regionRsc);
            for (int i = 0; i < subRegions.size(); i++) {
                Region aSubRegion = subRegions.get(i);
                if (i == 0) {
                    for (int j = 0; j < aSubRegion.getTimeSeries().sizeOfActivityArray(); j++) {
                        Activity activity = avgTimeSeries.addNewActivity();
                        activity.setTime(aSubRegion.getTimeSeries().getActivityArray(j).getTime());
                        activity.setFloatValue(0);
                    }
                }
                for (int j = 0; j < aSubRegion.getTimeSeries().sizeOfActivityArray(); j++) {
                    Activity activity = avgTimeSeries.getActivityArray(j);
                    activity.setFloatValue(activity.getFloatValue() + aSubRegion.getTimeSeries().getActivityArray(j).getFloatValue());
                }
            }
            for (int i = 0; i < avgTimeSeries.sizeOfActivityArray(); i++) {
                Activity activity = avgTimeSeries.getActivityArray(i);
                activity.setFloatValue(activity.getFloatValue()/subRegions.size());
            }
            rtn.put(regionRsc,avgRegion);
        }
        return rtn;
    }
    
    
    private Hashtable<RegionResource, ArrayList<Region>> groupByRegion() {
        Hashtable<RegionResource, ArrayList<Region>> rtn = new Hashtable<RegionResource,ArrayList<Region>>();
        PetTimeCourseData petTimeCourseData = petTimeCourseDoc.getPETTimeCourse();
        if (petTimeCourseData.isSetRegions()) {
            Regions regions = petTimeCourseData.getRegions();
            for(int i = 0; i < regions.sizeOfRegionArray(); i++ ) {
                Region aRegion = regions.getRegionArray(i);
                RegionResource regionRsc = getRegionResource(aRegion.getName(), aRegion.getHemisphere().toString());
                regionToRegionResourceHash.put(aRegion,regionRsc);
                if (rtn.containsKey(regionRsc)) {
                    rtn.get(regionRsc).add(aRegion);
                }else {
                    ArrayList<Region> regionList = new ArrayList<Region>();
                    regionList.add(aRegion);
                    rtn.put(regionRsc,regionList);
                }
                
            }
        }
        
        return rtn;
    }
    
    public String getLogPropertiesFile() {
        return (String)commandLineArgs.get("log");
    }
    
    
    public void printUsage() {
        String usage = "PETTimeSeriesAssessorCreator  \n";
        usage += "Options:\n";
        usage += "\t -sessionId <session label>\n";
        usage += "\t -xnatId <session XNAT id>\n";
        //usage += "\t -project <project>\n";
        usage += "\t -tislist: <Path to Time Series List File> \n";
        usage += "\t -u: XNAT username [Optional: will parse .xnatPass file] \n";
        usage += "\t -pwd: XNAT password [Optional: will parse .xnatPass file]\n";
        usage += "\t -host: URL to XNAT based Website\n";
        usage += "\t -buildDir: the path to directory where the session was built\n";
        usage += "\t -archiveDir: the path to session folder in the archive\n";
        usage += "\t -log <path to log4j.properties file>\n";
        usage += "\t -totalframes <total number of frames>\n";
        usage += "\t -startframes <startframe for this assessment>\n";
        //usage += "\t -defaultstartframe <the default start frame>\n";
        usage += "\t -help\n";

        System.out.println(usage);
        System.exit(1);
    }
    
    public static void main(String args[]) {
        PETTimeSeriesAssessorCreator petAssessor = new PETTimeSeriesAssessorCreator(args);
        if (petAssessor.getLogPropertiesFile() != null) {
            PropertyConfigurator.configure(petAssessor.getLogPropertiesFile());
        }else {
            BasicConfigurator.configure();
        }
        petAssessor.addTimeSeriesData();
        System.exit(0);
    }
}
