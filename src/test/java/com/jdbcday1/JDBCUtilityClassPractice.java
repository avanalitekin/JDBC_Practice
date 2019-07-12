package com.jdbcday1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;
import utilities.ConfigurationReader;
import utilities.DBUtility;
import utilities.DBUtility2;
import utilities.DBUtilsFromBookIt;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCUtilityClassPractice {

	@Test
	public void dBUtilityTest1() {
		
		String query="Select * from Employees";
		DBUtility2.getConnectionODB();
		DBUtility2.executeQuery(query);
		System.out.println("Column Names: "+DBUtility2.getColumnNames());
		System.out.println("Last row index: "+DBUtility2.getLastRowIndex());
		System.out.println("Data on column 1: "+DBUtility2.getDataOnAColumn(1));
		System.out.println("Data on column 2: "+DBUtility2.getDataOnAColumn(2));
		System.out.println("list of rowlist: "+DBUtility2.getRowList());
		System.out.println("list of rowlist size: "+DBUtility2.getRowList().size());
		System.out.println("List of row Maps: "+DBUtility2.getQueryResultMapList());
		System.out.println("List of row Maps size: "+DBUtility2.getQueryResultMapList().size());
		System.out.println("getQueryResultMapList().get(0): "+DBUtility2.getQueryResultMapList().get(0));
		System.out.println("getQueryResultMapList().get(10): "+DBUtility2.getQueryResultMapList().get(10));
		System.out.println("0th rowlist: "+DBUtility2.getRowList().get(0));
		System.out.println("10th rowlist: "+DBUtility2.getRowList().get(10));
		System.out.println("0th rowlist 0th object: "+DBUtility2.getRowList().get(0).get(0));
		DBUtility2.closeConnection();
		
		
		
	}
	
	@Test
	public void dBUtilityTest2() {
		String query=("SELECT * from employees WHERE department_id = 90");
		DBUtility2.getConnectionODB();
		DBUtility2.executeQuery(query);
		System.out.println("List of row Maps: "+DBUtility2.getQueryResultMapList());
		System.out.println("0th job id: "+DBUtility2.getQueryResultMapList().get(0).get("JOB_ID"));
		Assert.assertTrue(DBUtility2.getQueryResultMapList().get(0).get("JOB_ID").toString().startsWith("AD"));
		DBUtility2.closeConnection();
		
	}
	
}