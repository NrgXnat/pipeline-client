/* 
 *	Copyright Washington University in St Louis 2006
 *	All rights reserved
 * 	
 * 	@author Mohana Ramaratnam (Email: mramarat@wustl.edu)

*/

package org.nrg.pipeline.client.xnat;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.log4j.Logger;

public class XnatPassParser {
    
    Properties xnatPass;
    String passFilePath;
    
    String defaultLoginInfo;
    
    public XnatPassParser() {
        passFilePath = System.getProperty("user.home") + File.separator + ".xnatPass";
        xnatPass = new Properties();
        load();
    }
    
    public boolean load() {
        boolean success = true;
        try {
            FileInputStream filein = new FileInputStream (passFilePath);
            DataInputStream input = new DataInputStream (filein);
            xnatPass.load(input);
            Enumeration e = xnatPass.keys();
            while (e.hasMoreElements()){
                String prop = (String) e.nextElement();
                if (prop.startsWith("+")) {
                    defaultLoginInfo = prop + "=" + xnatPass.getProperty(prop);
                    break;
                }
            }
        }catch(FileNotFoundException fne) {
            success = false;
        }catch(IOException ioe) {
            success = false;
        }
        return success;
    }
    

    public String getDefaultLoginInfo() {
        return defaultLoginInfo;
    }
    
    
    
    public String getLoginInfo(String host) {
        String rtn = null;
        if (host == null) return "";
        String h = transform(host);
        Enumeration e = xnatPass.keys();
        while (e.hasMoreElements()){
            String prop = (String) e.nextElement();
            if (prop.endsWith(h)) {
                rtn = prop + "=" + xnatPass.getProperty(prop);
                break;
            }
        }
        if (rtn == null) {
            logger.error("Couldnt get information for host ");
        }
        return rtn;
    }
    
    public String getUserAtHost(String loginInfo) {
        String rtn = null;
        if (loginInfo == null) {
            logger.error("Couldnt parse userAtHost from " + loginInfo);
            return rtn;
        }
        String parts[] = loginInfo.split("=");
        if (parts==null || parts.length < 1 ) {
            logger.error("Couldnt parse userAtHost from " + loginInfo + " invalid format. Expecting *@*=*");
            return rtn;
        }
        rtn  = parts[0];
        if (rtn.startsWith("+")) rtn = rtn.substring(1);
        return rtn;
    }
    
    public String getHost(String loginInfo) {
        String rtn = null;
        if (loginInfo == null) {
            logger.error("Couldnt parse userAtHost from " + loginInfo);
            return rtn;
        }
        String parts[] = loginInfo.split("=");
        if (parts==null || parts.length < 1 ) {
            logger.error("Couldnt parse userAtHost from " + loginInfo + " invalid format. Expecting *@*=*");
            return rtn;
        }
        rtn  = parts[0];
        parts = rtn.split("@");
        if (parts==null || parts.length < 2 ) {
            logger.error("Couldnt parse userAtHost from " + loginInfo + " invalid format. Expecting *@*");
            return rtn;
        }
        rtn  = "http://" + parts[1];
        return rtn;
    }

    
    public String getUser(String userAtHost) {
        String rtn = null;
        if (userAtHost == null) {
            logger.error("Couldnt parse userAtHost from " + userAtHost);
            return rtn;
        }
        String parts[] = userAtHost.split("@");
        if (parts==null || parts.length < 1 ) {
            logger.error("Couldnt parse userAtHost from " + userAtHost + " invalid format. Expecting *@*");
            return rtn;
        }
        rtn  = parts[0];
        if (rtn.startsWith("+")) rtn = rtn.substring(1);
        return rtn;
    }

    
    public String getPassword(String loginInfo) {
        String rtn = null;
        if (loginInfo == null) {
            logger.error("Couldnt parse password from " + loginInfo);
            return rtn;
        }
        String parts[] = loginInfo.split("=");
        if (parts==null || parts.length < 1 ) {
            logger.error("Couldnt parse password from " + loginInfo + " invalid format. Expecting *@*=*");
            return rtn;
        }
        return parts[1];
    }
    
    private String transform(String host) {
        if (host.startsWith("http://")) {
            host = host.replaceAll("http://","@");
        }
        if (!host.startsWith("@")) host = "@" + host;
        if (!host.endsWith("/")) {
            host += "/";
        }
        return host;
    }
    
    
    
    static Logger logger = Logger.getLogger(XnatPassParser.class);
    
}
