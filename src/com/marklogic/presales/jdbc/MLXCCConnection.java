package com.marklogic.presales.jdbc;

import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;

public class MLXCCConnection implements Connection {
  String user = "";
  String pass = "";
  String hostname = "";
  String port = "";
  Properties properties = new Properties();
  

  ContentSource cs = null;
  
  // JDBC spec support
  Properties clientInfo = new Properties();
  
  public MLXCCConnection(String user,String pass,String hostname,String port,String database,Properties ourProps) {
    this.user = user;
    this.pass = pass;
    this.hostname = hostname;
    this.port = port;
    this.properties = ourProps;
    
    String serverUri = "xcc://" + user + ":" + pass + "@" + hostname + ":" + port + "/" + database;
    System.out.println("MLXCCConnection: " + serverUri);
    
    try {
      cs = ContentSourceFactory.newContentSource(new URI(serverUri));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public Statement createStatement() throws SQLException {
    return new MLXCCStatement(this,cs);
  }

  
  
  

  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    return false;
  }

  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new SQLException("Does not support wrapping interfaces");
  }

  public void clearWarnings() throws SQLException {
    // do nothing
  }

  public void close() throws SQLException {
    // do nothing - in future could close all statements
  }

  public void commit() throws SQLException {
    // do nothing
  }

  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }
  
  public Statement createStatement(int arg0, int arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public Statement createStatement(int arg0, int arg1, int arg2)
      throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  public String getCatalog() throws SQLException {
    return null; // no name
  }

  public Properties getClientInfo() throws SQLException {
    return clientInfo;
  }

  public String getClientInfo(String arg0) throws SQLException {
    return clientInfo.getProperty(arg0);
  }

  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    return null; // TODO validate this is ok
  }

  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  public boolean isClosed() throws SQLException {
    return false;
  }

  public boolean isReadOnly() throws SQLException {
    return true;
  }

  public boolean isValid(int arg0) throws SQLException {
    return true;
  }

  public String nativeSQL(String arg0) throws SQLException {
    return arg0; // keep the same - do not account for ? parameters
  }

  public CallableStatement prepareCall(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public CallableStatement prepareCall(String arg0, int arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public PreparedStatement prepareStatement(String arg0) throws SQLException {
    return new MLXCCStatement(this,cs,arg0);
  }

  public PreparedStatement prepareStatement(String arg0, int arg1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String arg0, int[] arg1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String arg0, String[] arg1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
      int arg3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void releaseSavepoint(Savepoint arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void rollback() throws SQLException {
    // do nothing - auto commit is on, so this method has no relevance
  }

  public void rollback(Savepoint arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public void setAutoCommit(boolean arg0) throws SQLException {
    // do nothing
  }

  public void setCatalog(String arg0) throws SQLException {
    // silently ignore as per jdbc spec
  }

  public void setClientInfo(Properties arg0) throws SQLClientInfoException {
    // do nothing
  }

  public void setClientInfo(String arg0, String arg1)
      throws SQLClientInfoException {
    // do nothing
  }

  public void setHoldability(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public void setReadOnly(boolean arg0) throws SQLException {
    // do nothing - read only, only
  }

  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public Savepoint setSavepoint(String arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

  public void setTransactionIsolation(int arg0) throws SQLException {
    // do nothing
  }

  public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException ();
  }

}
