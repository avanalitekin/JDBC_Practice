package com.jdbcday1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;
import utilities.ConfigurationReader;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCDay1 {

	/*
	 * <dependency> <groupId>mysql</groupId>
	 * <artifactId>mysql-connector-java</artifactId> <version>8.0.15</version>
	 * </dependency>
	 * 
	 */

//    connection_tool=jdbc
//    database_type=postgresql
//    host = room-reservation-qa.cxvqfpt4mc2y.us-east-1.rds.amazonaws.com
//    port = 5432
//    database_name = hr
//	  String url = "jdbc:postgresql://room-reservation-qa.cxvqfpt4mc2y.us-east-1.rds.amazonaws.com:5432/hr";

//    String username = "hr";
//    String password = "hr";

//    String url = "jdbc:postgresql://room-reservation-qa.cxvqfpt4mc2y.us-east-1.rds.amazonaws.com:5432/hr";
//    String username = "hr";
//    String password = "hr";
	Connection connection;
	Statement statement;
	private static Logger logger = LogManager.getLogger(JDBCDay1.class);

	@Before
	public void setUp() throws SQLException {
		String url = ConfigurationReader.getProperty("mypostgredb_url");
		String username = ConfigurationReader.getProperty("mypostgredb_un");
		String password = ConfigurationReader.getProperty("mypostgredb_pw");
		connection = DriverManager.getConnection(url, username, password);
		statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

	}

@Test
	public void jdbcTest1() throws SQLException {
		ResultSet resultSet = statement.executeQuery("Select * from Employees;");
		resultSet.next();
		System.out.println(resultSet.getString("first_name"));
		System.out.println(resultSet.getString("last_name"));
		resultSet.next();
		System.out.println(resultSet.getString("first_name"));
		System.out.println(resultSet.getString("last_name"));

		System.out.println();
		resultSet.beforeFirst();
		while (resultSet.next()) {
			System.out.println(resultSet.getRow() + ":: " + resultSet.getString("first_name") + " "
					+ resultSet.getString("last_name"));
		}
	}

//	@Test
	public void jdbctest2() throws SQLException {
		ResultSet resultset = statement.executeQuery("Select * from Locations;");
		resultset.absolute(1);
		System.out.println(resultset.getRow() + " : " + resultset.getObject("City"));
		resultset.last();
		System.out.println(resultset.getRow() + " : " + resultset.getObject("City"));

	}

//	@Test
	public void jdbctest3() throws SQLException {
		ResultSet resultset = statement.executeQuery("Select * from Employees;");
		resultset.last();
		int actualRowsCount = resultset.getRow();
		int expectedRowsCount = 107;
		logger.trace("Assertion"); // logger.file.level = off change with logger.file.level = trace or info or ...
		logger.info("Assertion"); // rootLogger.level = off change with rootLogger.level = info or trace or ....
		logger.error("Assertion");
		logger.warn("Assertion");
		logger.fatal("Assertion");
		Assert.assertEquals(actualRowsCount, expectedRowsCount);
	}

//	@Test
	public void getMetaDataTest() throws SQLException {
		DatabaseMetaData dbMetaData = connection.getMetaData();
		String dbName = dbMetaData.getDatabaseProductName();
		String dbVersion = dbMetaData.getDatabaseProductVersion();
		String dbDriverName = dbMetaData.getDriverName();
		String dbURL = dbMetaData.getURL();
		String dbUsername = dbMetaData.getUserName();
//		ResultSet dbSchemas=dbMetaData.getSchemas();
		System.out.println("dbName: " + dbName);
		System.out.println("dbVersion: " + dbVersion);
		System.out.println("dbDriverName: " + dbDriverName);
		System.out.println("dbURL: " + dbURL);
		System.out.println("dbUsername: " + dbUsername);

	}

//	@Test
	public void getResultSetMetaData() throws SQLException {
		ResultSet resultset = statement.executeQuery("Select * from  Employees;");
		ResultSetMetaData rsMetaData = resultset.getMetaData();
//		System.out.println("rsMetaData.getColumnCount(): "+rsMetaData.getColumnCount());
		for (int columnIndex = 1; columnIndex <= rsMetaData.getColumnCount(); columnIndex++) {
			System.out.println("column " + columnIndex + " name: " + rsMetaData.getColumnName(columnIndex));
		}
	}

	@Test
	public void getAllTableData() throws Exception {
		ResultSet resultset = statement.executeQuery("Select * from Employees;");
		ResultSetMetaData resultSetMetaData = resultset.getMetaData();
		List<Map<String, Object>> employeesTable = new ArrayList<Map<String, Object>>();
		while (resultset.next()) {
			Map<String, Object> rowMap = new HashMap<String, Object>();
			int columnIndex = 1;
			while (columnIndex <= resultSetMetaData.getColumnCount()) {
				rowMap.put(resultSetMetaData.getColumnName(columnIndex), resultset.getObject(columnIndex));
				columnIndex++;
			}
			employeesTable.add(rowMap);
		}
		System.out.println(employeesTable);
		System.out.println(employeesTable.size());
	}

	@After
	public void tearDown() throws SQLException {
		connection.close();
	}
}