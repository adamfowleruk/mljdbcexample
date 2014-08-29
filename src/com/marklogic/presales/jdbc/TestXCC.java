package com.marklogic.presales.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestXCC {

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      // fetch connection
      Class cl = Class.forName("com.marklogic.presales.jdbc.MLXCCDriver");
      
      Connection c = DriverManager.getConnection("jdbc:mlxcc:localhost:8088/restaurant?username=admin&password=admin");
      
      // execute 
      
      String sql = "select * from v_restaurant where SqFtEst>=1500 or 1=1 order by ID asc"; // SQLI add ' or 1=1 ' after 1500 -> returns 291 instead of 196
      Statement s = c.createStatement();
      ResultSet rs = s.executeQuery(sql);
      
      // process rs
      int idCol = rs.findColumn("ID");
      int sqFtEstCol = rs.findColumn("SqFtEst");
      int stateCol = rs.findColumn("State");
      int count = 0;
      while (rs.next()) {
        count++;
        System.out.println("ROW ID: " + rs.getString(idCol) + ", State: " + rs.getString(stateCol) + ", SqFt: " + rs.getString(sqFtEstCol) );
      }
      System.out.println("TOTAL: " + count);
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }

}
