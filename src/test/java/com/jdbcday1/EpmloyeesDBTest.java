package com.jdbcday1;

import org.junit.Assert;
import org.junit.Test;

import utilities.DBUtility;

import static utilities.DBUtility.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static utilities.ConfigurationReader.*;

public class EpmloyeesDBTest {
//  @Test
  public void countTest() throws SQLException {
	  DBUtility.createConnection(getProperty("mypostgredb_url"), getProperty("mypostgredb_un"), getProperty("mypostgredb_pw"));
	  List<Map<String, Object>> itProgList=DBUtility.getQueryMapList("select * from employees where job_id='IT_PROG';");
	  System.out.println(itProgList);
	  int rowCount=DBUtility.getRowCount("select * from employees where job_id='IT_PROG';");
	  Assert.assertTrue(rowCount>0);
	  DBUtility.closeConnections();
  }
  
  @Test
  public void namesTestByID() throws SQLException {
	  DBUtility.createConnection(getProperty("mypostgredb_url"), getProperty("mypostgredb_un"), getProperty("mypostgredb_pw"));
	  List<Map<String,Object>> employeeName=DBUtility.getQueryMapList("select first_name,last_name from employees where employee_id=105;");
	  System.out.println(employeeName);
	  Assert.assertEquals("David Austin", employeeName.get(0).get("first_name")+" "+employeeName.get(0).get("last_name"));
	  }
  
}
