package com.jdbcday1;

import static utilities.ConfigurationReader.getProperty;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.*;

import utilities.ConfigurationReader;

public class JDBCOnOracleDB {
	
	Connection connection;
	Statement statement;
	
	@Before
	public void setUp() throws SQLException {
	String url=ConfigurationReader.getProperty("myoracledb_url");
	String username=ConfigurationReader.getProperty("myoracledb_un");
	String password=ConfigurationReader.getProperty("myoracledb_pw");
	connection=DriverManager.getConnection(url, username, password);
	statement=connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}
	
	@Test
	public void jdbctest1() throws SQLException {
	ResultSet resultSet=statement.executeQuery("Select * from Employees");
	
	while(resultSet.next()) {
		System.out.println(resultSet.getRow()+" :: "+resultSet.getObject("first_name")+" "+resultSet.getObject("last_name"));
	}
	
	
	System.out.println("==================");
//	resultSet.first();
	System.out.println(resultSet.first()+"\n"+resultSet.getRow());
	}
	
	@Test
	public void jdbctest2() throws SQLException {
		ResultSet resultSet=statement.executeQuery("Select * from locations");
		resultSet.next();
		String city=resultSet.getString("city");
		System.out.println("city: "+city);
		
		resultSet.beforeFirst();//statement in while loop will take it to the next row
		while(resultSet.next()) {
			System.out.println(resultSet.getRow()+" :: "+resultSet.getString("city"));
		}
		resultSet.last();
		System.out.println("city: "+resultSet.getString("city"));
		
		resultSet.absolute(20);
		String cityExpected="Geneva";
		String cityActual=resultSet.getString("city");
		System.out.println("city in row 20: "+resultSet.getString("city"));
		Assert.assertEquals(cityExpected, cityActual);
		
		
	}
	
	
	@Test
	public void jdbctest3() throws SQLException {
		ResultSet resultSet=statement.executeQuery("Select * from employees");
		resultSet.last();
		int actualRowCount=resultSet.getRow();
		int expectedRowCount=107;
		Assert.assertEquals(expectedRowCount, actualRowCount);
	}
	
	@Test
	public void jdbctest4() throws SQLException {
		DatabaseMetaData dbMetaData=connection.getMetaData();
		System.out.println("Username: "+dbMetaData.getUserName());
		System.out.println("Database Name: "+dbMetaData.getDatabaseProductName());
		System.out.println("URL: "+dbMetaData.getURL());
		System.out.println("Database version: "+dbMetaData.getDatabaseProductVersion());
		System.out.println("Driver name: "+dbMetaData.getDriverName());
	
	}
	
	@Test
	public void jdbctest5() throws SQLException {
		ResultSet resultSet=statement.executeQuery("Select * from employees");
		ResultSetMetaData resultSetMetaData=resultSet.getMetaData();
		System.out.println(resultSetMetaData.getColumnCount());
		System.out.println(resultSetMetaData.getColumnDisplaySize(2));
		System.out.println(resultSetMetaData.getColumnName(2));
		int count=1;
		while(count<=resultSetMetaData.getColumnCount()) {
			System.out.print(resultSetMetaData.getColumnName(count++));
		System.out.print(" | ");
		}
		
				
	}
	@Test
	public void getQueryMap() throws SQLException {
	ResultSet resultSet=statement.executeQuery("Select* from employees");
	ResultSetMetaData resultSetMetaData=resultSet.getMetaData();
	List<Map<String,Object>> employeesTable=new ArrayList<Map<String, Object>>();
	
	while(resultSet.next()) {
		Map<String,Object> rowMap=new HashMap<String, Object>();
		int column=0;
		while(column++<resultSetMetaData.getColumnCount()) {
			rowMap.put(resultSetMetaData.getColumnName(column), resultSet.getObject(column));
		}
		employeesTable.add(rowMap);
	}
	
	for (Map<String, Object> row:employeesTable) {
		System.out.println(row);
	}
	System.out.println("\n=====================================================\n");
	for (int i=0; i<employeesTable.size(); i++) {
		System.out.println(employeesTable.get(i));
	}
	System.out.println(employeesTable.size());
	System.out.println("\n=====================================================\n");
	for (Map<String, Object> row:employeesTable) {
		System.out.println(row.get("EMPLOYEE_ID"));
	}
	}
	
	@After
	public void tearDown() throws SQLException {
		statement.close();
		connection.close();
	}
	
	

}
