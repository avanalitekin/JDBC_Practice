package utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBUtility {
	private static Connection connection;
	private static Statement statement;
	private static ResultSet resultSet;
	
	public static void createConnection(String url, String username, String password) {
		try {
			connection = DriverManager.getConnection(url, username, password);
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public static void closeConnections() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public static void executeQuery(String query) {
		try {
			statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			resultSet = statement.executeQuery(query);
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public static int getRowCount(String query) {
		executeQuery(query);
		int rowCount = 0;
		try {
			resultSet.last();
			rowCount = resultSet.getRow();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		return rowCount;
	}

	public static List<String> getColumnNames(String query) {
		List<String> columnNames = new ArrayList<>();
		try {
			executeQuery(query);
			ResultSetMetaData rsmd = resultSet.getMetaData();
			int columnCount = rsmd.getColumnCount();
			int column = 0;
			while (column++ <columnCount) {
				columnNames.add(rsmd.getColumnName(column));
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		return columnNames;
	}

	public static List<Map<String, Object>> getQueryMapList(String query) {
		executeQuery(query);
		List<Map<String, Object>> queryList = new ArrayList<Map<String, Object>>();
		try {
			ResultSetMetaData rsmd = resultSet.getMetaData();
			while (resultSet.next()) {
				Map<String, Object> rowMap = new HashMap<String, Object>();
				int column = 0;
				while (column++ < rsmd.getColumnCount()) {
					rowMap.put(rsmd.getColumnName(column), resultSet.getObject(column));
				}
				queryList.add(rowMap);
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		return queryList;
	}

}
