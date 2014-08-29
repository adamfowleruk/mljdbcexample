package com.marklogic.presales.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Vector;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.types.XdmElement;

public class MLXCCStatement implements PreparedStatement {
  
  protected Session session = null;
  protected String sql = "";
  protected String fs = "";
  
  protected Connection connection = null;
  
  protected ResultSet resultSet = null;
  
  boolean closed = false;
  
  public MLXCCStatement(Connection conn,ContentSource source) {
    connection = conn;
    session = source.newSession();
  }
  
  public MLXCCStatement(Connection conn,ContentSource source,String sql) {
    connection = conn;
    session = source.newSession();
    setSql(sql);
  }
  
  
  


  public ResultSet executeQuery(String arg0) throws SQLException {
    setSql(arg0);
    return doQuery();
  }
  
  protected void setSql(String sql) {
    this.sql = sql;
    fs = "xquery version \"1.0-ml\";declare namespace json=\"http://marklogic.com/xdmp/json\";let $ret := xdmp:sql(\"" + sql + 
        "\") return element results {$ret}";
  }
  
  protected ResultSet doQuery() throws SQLException {
    //Request q = session.newAdhocQuery("xdmp:sql(\"" + arg0 + "\")[1]");
    
    Request q = session.newAdhocQuery(fs);
    try {
      MLResultSet mlrs = null;
      ResultSequence rs = session.submitRequest(q);
      ResultItem ri;
      while (null != (ri = rs.next())) {
        Element el = ((XdmElement)ri.getItem()).asW3cElement();
        mlrs = processResult(el);
      }
      resultSet = mlrs;
      return mlrs;
    } catch (Exception e) {
      throw new SQLException (e);
    }
  }
  
  private static MLResultSet processResult(Element el) {
    MLResultSet mls = new MLResultSet();
    // <result><json:array><json:value> - First row values are columns
    // process first row (column names)
    NodeList arrays = el.getChildNodes();
    Vector<String> columnNames = new Vector<String>();
    
    Element columnNameArr = (Element)arrays.item(0);
    NodeList columnNodes = columnNameArr.getChildNodes();
    for (int cn = 0;cn < columnNodes.getLength();cn++) {
      Node n = columnNodes.item(cn);
      if (n instanceof Element) {
        columnNames.add(n.getChildNodes().item(0).getNodeValue());
      }
    }
    
    mls.setColumnNames(columnNames);
    System.out.println(columnNames.toString());
    
    // process remaining rows
    for (int i = 1; i < arrays.getLength();i++) {
      Node rowNode = arrays.item(i);
      NodeList cols = rowNode.getChildNodes();
      for (int c = 0;c < cols.getLength();c++) {
        Node n = cols.item(c);
        if (n instanceof Element) {
          mls.addRowValue(n.getChildNodes().item(0).getNodeValue());
        }
      }
      mls.addRow();
    }
    
    return mls;
  }
  
  
  
  // TODO next methods

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  public void addBatch(String arg0) throws SQLException {
    // do nothing
  }

  public void cancel() throws SQLException {
    // do nothing
  }

  public void clearBatch() throws SQLException {
    // do nothing
  }

  public void clearWarnings() throws SQLException {
    // do nothing
  }

  public void close() throws SQLException {
    closed = true;
    session.close();
  }

  public boolean execute(String arg0) throws SQLException {
    ResultSet res = executeQuery(arg0);
    if (null != res) {
      return true;
    } else {
      return false;
    }
  }

  public boolean execute(String arg0, int arg1) throws SQLException {
    return execute(arg0);
  }

  public boolean execute(String arg0, int[] arg1) throws SQLException {
    return execute(arg0);
  }

  public boolean execute(String arg0, String[] arg1) throws SQLException {
    return execute(arg0);
  }

  public int[] executeBatch() throws SQLException {
    return new int[]{};
  }

  public int executeUpdate(String arg0) throws SQLException {
    throw new SQLException("MarkLogic SQL does not support updates.");
  }

  public int executeUpdate(String arg0, int arg1) throws SQLException {
    throw new SQLException("MarkLogic SQL does not support updates.");
  }

  public int executeUpdate(String arg0, int[] arg1) throws SQLException {
    throw new SQLException("MarkLogic SQL does not support updates.");
  }

  public int executeUpdate(String arg0, String[] arg1) throws SQLException {
    throw new SQLException("MarkLogic SQL does not support updates.");
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public int getMaxFieldSize() throws SQLException {
    // no limit = 0
    return 0;
  }

  public int getMaxRows() throws SQLException {
    // no limit = 0
    return 0;
  }

  public boolean getMoreResults() throws SQLException {
    // only return single result set (as this set is unlimited)
    return false;
  }

  public boolean getMoreResults(int arg0) throws SQLException {
    // only return single result set (as this set is unlimited)
    return false;
  }

  public int getQueryTimeout() throws SQLException {
    // no limit = 0
    return 0;
  }

  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  public int getResultSetConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_SCROLL_INSENSITIVE; // need to validate this
  }

  public int getUpdateCount() throws SQLException {
    return -1; // updatest not supported
  }

  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public boolean isPoolable() throws SQLException {
    return true; // can't think why not
  }

  public void setCursorName(String arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setEscapeProcessing(boolean arg0) throws SQLException {
    // do nothing
  }

  public void setFetchDirection(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  public void setFetchSize(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  public void setMaxFieldSize(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  public void setMaxRows(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  public void setPoolable(boolean arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  public void setQueryTimeout(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  public void addBatch() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void clearParameters() throws SQLException {
    // TODO Auto-generated method stub
    
  }
  
  
  
  // PREPARED STATEMENT METHODS

  public boolean execute() throws SQLException {
    doQuery();
    return true;
  }

  public ResultSet executeQuery() throws SQLException {
    return doQuery();
  }

  public int executeUpdate() throws SQLException {
    return 0;
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    return null; // validate this is ok
  }

  public void setArray(int arg0, Array arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setAsciiStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setAsciiStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBinaryStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBinaryStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBlob(int arg0, Blob arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBlob(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBlob(int arg0, InputStream arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBoolean(int arg0, boolean arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setByte(int arg0, byte arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setBytes(int arg0, byte[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setCharacterStream(int arg0, Reader arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setCharacterStream(int arg0, Reader arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setClob(int arg0, Clob arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setClob(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setDate(int arg0, Date arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setDouble(int arg0, double arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setFloat(int arg0, float arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setInt(int arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setLong(int arg0, long arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNCharacterStream(int arg0, Reader arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNClob(int arg0, NClob arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNClob(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNString(int arg0, String arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNull(int arg0, int arg1, String arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setNull(int arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setObject(int arg0, Object arg1, int arg2, int arg3)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setObject(int arg0, Object arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setRef(int arg0, Ref arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setRowId(int arg0, RowId arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setShort(int arg0, short arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setString(int arg0, String arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setTime(int arg0, Time arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setURL(int arg0, URL arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  public void setUnicodeStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }
  
  
  
  // PREPARED STATEMENT METHODS
  

}
