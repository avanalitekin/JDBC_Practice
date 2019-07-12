package com.jdbcday1;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.*;


public class JDBCDay1Copy {
    //host = room-reservation-qa.cxvqfpt4mc2y.us-east-1.rds.amazonaws.com
    //database = hr
    //user = hr
    //password = hr
    //port = 5432 (default for postgres)
//	String connectionString = "jdbc:postgresql://host:port/database";
//	String username = "hr";
//	String password = "hr";


    String url = "jdbc:postgresql://room-reservation-qa.cxvqfpt4mc2y.us-east-1.rds.amazonaws.com:5432/hr";
    String username = "hr";
    String password = "hr";
    Connection connection;
    Statement statement;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
    	//When we are working with more than one data base in our automation framework, we need to specify
    	//To make sure that we are using correct database driver for JDBC
    	Class.forName("org.postgresql.Driver");
        //Now we are connecting to the data base from eclispe/intellij with java code and JDBC Api
        connection = DriverManager.getConnection(url, username, password);
        //We need to create a statement
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

//    @Test
    public void jdbcTest1() throws SQLException{
        //Now we gonna run a query, so for that we need to create a result set
        ResultSet resultSet = statement.executeQuery("SELECT * FROM employees;");
        //next() method returns boolean, so we can get value based on the column name
        while(resultSet.next()) {
            System.out.println(resultSet.getRow()+" :: "+resultSet.getObject("first_name"));
        }
        //go to first row
        resultSet.first();
        System.out.println("--------------");
        System.out.println(resultSet.getRow());
    }

//    @Test
    public void jdbctest2() throws SQLException {
        //Now we gonna run a query, so for that we need to create a result set
        ResultSet resultSet = statement.executeQuery("SELECT * FROM locations;");
        //we need to skip first record, because it start from 0
        resultSet.next();
        //we are getting first record based on the column name
        String value = resultSet.getString("city");
     
        resultSet.beforeFirst();
        while(resultSet.next()) {
            System.out.println(resultSet.getRow()+" :: "+resultSet.getObject("city"));
        }
        System.out.println();
        //just to output result int oterminal
        System.out.println(value);
      
        //if we are calling this method, it will move out of the list of records. Once we will try to get a data, we will get an exception
        //resultSet.afterLast();
        //last method moves to the end and we can get data of last record
        resultSet.last();
        String value2 = resultSet.getString("city");
        //just to output result int oterminal
        System.out.println();
        System.out.println(value2);
        //this method moves to the specific row
        resultSet.absolute(20);
        System.out.println();
        String actualCity = resultSet.getString("city");
        //just to output result into terminal
        System.out.println(actualCity);
        Assert.assertEquals("Geneva", actualCity);
    }

//    @Test
    public void jdbctest3() throws SQLException {
        //we want to verify that there are 107 employees
        ResultSet resultSet = statement.executeQuery("SELECT * FROM employees;");
        //we need to go to the last row
        resultSet.last();
        int actualAmountOfRows = resultSet.getRow();
        int expectedAmountOfRows = 107;
        Assert.assertEquals(expectedAmountOfRows, actualAmountOfRows);
    }

    //Metadata about data base
//    @Test
    public void jdbctest4() throws SQLException {
        //so we want to get metadata about DB
        DatabaseMetaData dbMetaData = connection.getMetaData();
        System.out.println("Username: " + dbMetaData.getUserName());
        String expcetedDBType = "PostgreSQL";
        String actualDBType = dbMetaData.getDatabaseProductName();
        Assert.assertEquals(expcetedDBType, actualDBType);
        System.out.println(actualDBType+" : : " + dbMetaData.getDatabaseProductVersion());
    }

    //Metadata about resultset, means metadata about or query
//    @Test
    public void jdbctest5() throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT * FROM employees;");
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        System.out.println(resultSetMetaData.getColumnCount());
        System.out.println(resultSetMetaData.getColumnName(2));
    }

//    @Test
    public void jdbctest6() throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT * FROM employees;");
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int numOfComun = resultSetMetaData.getColumnCount();
        for(int i = 1 ; i <= numOfComun ; i ++) {
            System.out.println("Name of a specific column with index: "+resultSetMetaData.getColumnName(i));
        }


    }

    @After
    public void teardown() throws SQLException {
        //TO close stream of data (connection)
        connection.close();
    }
}