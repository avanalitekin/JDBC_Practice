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

public class DBUtility2 {
	private static Connection connection;
	private static Statement statement;
	private static ResultSet resultSet;
	private static ResultSetMetaData rsmd;

	public static void getConnectionODB() {
		String url = ConfigurationReader.getProperty("myoracledb_url");
		String username = ConfigurationReader.getProperty("myoracledb_un");
		String password = ConfigurationReader.getProperty("myoracledb_pw");
		try {
			connection = DriverManager.getConnection(url, username, password);
			statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void executeQuery(String query) {
		try {
			resultSet = statement.executeQuery(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void closeConnection() {
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

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static List<Object> getColumnNames() {
		List<Object> columnNames = new ArrayList<Object>();

		try {
			rsmd = resultSet.getMetaData();
			int column = 1;
			while (column <= rsmd.getColumnCount()) {
				columnNames.add(rsmd.getColumnName(column++));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return columnNames;
	}

	public static int getLastRowIndex() {

		int rowIndex = -1;
		try {
			resultSet.last();
			rowIndex = resultSet.getRow();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rowIndex;

	}

	public static List<Object> getDataOnAColumn(int column) {
		List<Object> columnData = new ArrayList<Object>();

		try {
			resultSet.beforeFirst();
			while (resultSet.next()) {
				columnData.add(resultSet.getObject(column));

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return columnData;
	}

	public static List<List<Object>> getRowList() {
		List<List<Object>> allRowList = new ArrayList<List<Object>>();
		try {
			rsmd = resultSet.getMetaData();
			resultSet.beforeFirst();
			while (resultSet.next()) {
				List<Object> rowList = new ArrayList<>();
				int column = 1;
				while (column <= rsmd.getColumnCount()) {
					rowList.add(resultSet.getObject(column++));
				}
				allRowList.add(rowList);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return allRowList;
	}

	public static List<Map<String, Object>> getQueryResultMapList() {
		List<Map<String, Object>> allRowMapList = new ArrayList<Map<String, Object>>();
		try {
			rsmd = resultSet.getMetaData();

			resultSet.beforeFirst();
			while (resultSet.next()) {
				Map<String, Object> rowMap = new HashMap<String, Object>();
				int column = 1;
				while (column <= rsmd.getColumnCount()) {
					rowMap.put(rsmd.getColumnName(column), resultSet.getObject(column++));
				}
				allRowMapList.add(rowMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();

		}
		return allRowMapList;
	}
}
