package com.marklogic.presales.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Implements a Type 3 (Network Protocol) JDBC Driver via the XCC API in order to allow arbitrary execution of sql using xdmp:sql in ML version 6.
 * 
 * @author adamfowler
 *
 */
public class MLXCCDriver implements Driver {
  static
  {
    try
    {
      // Register the JWDriver with DriverManager
      MLXCCDriver driverInst = new MLXCCDriver();
      DriverManager.registerDriver(driverInst);
      //System.setSecurityManager(new RMISecurityManager());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Accepts jdbc:mlxcc:localhost:8088 or jdbc:mlxcc:localhost:8088/?username=adam&password=easy URLs
   */
  public Connection connect(String arg0, Properties arg1) throws SQLException {
    String user = "";
    String pass = "";
    String hostname = "";
    String port = "";
    String database = "";
    
    Properties ourProps = (Properties)arg1.clone();
    
    String[] parts = arg0.split(":");
    if (parts.length < 4) {
      throw new SQLException("Not enough parts to JDBC URL. Try: jdbc:mlxcc:localhost:8088 or jdbc:mlxcc:localhost:8088/?username=adam&password=easy");
    }
    
    hostname = parts[2];
    int idx = parts[3].indexOf("/");
    if (-1 != idx) {
      port = parts[3].substring(0,idx);
      String remainder = parts[3].substring(idx+1);
      idx = remainder.indexOf("/");
      if (-1 == idx) {
        idx = remainder.indexOf("?");
        if (-1 == idx) {
          // whole string is db
          database = remainder;
          remainder = "";
        } else {
          database = remainder.substring(0,idx);
          remainder = remainder.substring(idx+1);
        }
      } else {
        int idx2 = remainder.indexOf("?");
        if (-1 == idx2) {
          // whole string is db
          database = remainder.substring(idx+1);
          remainder = "";
        } else {
          database = remainder.substring(idx+1,idx2);
          remainder = remainder.substring(idx2+1);
        }
      }
      
      if (remainder.startsWith("?")) {
        remainder = remainder.substring(1);
      }
      
      // copy URL properties to properties object, incase they were on URL instead of in Properties
      System.out.println("remainder: " + remainder);
      if (!"".equals(remainder.trim())) {
        String params[] = remainder.split("&");
        for (int p = 0;p < params.length;p++) {
          String nvp[] = params[p].split("=");
          ourProps.put(nvp[0], nvp[1]);
        }
      }

    } else {
      port = parts[3];
    }
    user = ourProps.getProperty("username");
    pass = ourProps.getProperty("password");
    return new MLXCCConnection(user,pass,hostname,port,database,ourProps);
  }
  
  
  public boolean acceptsURL(String arg0) throws SQLException {
    return arg0.startsWith("jdbc:mlxcc:");
  }
  
  
  
  
  // other methods

  public int getMajorVersion() {
    return 0;
  }

  public int getMinorVersion() {
    return 2;
  }

  public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
      throws SQLException {
    return new DriverPropertyInfo[]{};
  }

  public boolean jdbcCompliant() {
    return true;
  }

}
