import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class ImportToHive {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public static Connection getConnection() throws SQLException {
		Connection con = DriverManager.getConnection(
				"jdbc:hive://192.168.1.103:10003/default", "hive", "");
		return con;
	}

	/*
	 * public static void selectAllElem(String tableName) throws SQLException {
	 * Connection connection = getConnection(); Statement stmt =
	 * connection.createStatement(); String sql = "select * from " + tableName;
	 * System.out.println("Running: " + sql); ResultSet res =
	 * stmt.executeQuery(sql); while (res.next()) {
	 * System.out.println(String.valueOf("userid: " + res.getInt(1)) + "\t" +
	 * "newsid: " + res.getInt(2) + "\t" + "interval: " + res.getInt(3)); } }
	 */

	public static void importNewsData() throws SQLException {
		Connection connection = getConnection();
		Statement stmt = connection.createStatement();
		String tableName = "usernews";
		stmt.executeQuery("drop table if exists " + tableName);
		ResultSet res = stmt
				.executeQuery("create table "
						+ tableName
						+ " (userid int, newsid int, visit_timestamp bigint, visit_time string, post_timestamp bigint, post_time string, news_title string, news_content string) row format delimited fields terminated by '\t'");
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/root/newsdata.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
	}

	public static void importOldestVisit() throws SQLException {
		Connection connection = getConnection();
		Statement stmt = connection.createStatement();
		String tableName = "oldestvisit";
		stmt.executeQuery("drop table if exists " + tableName);
		ResultSet res = stmt
				.executeQuery("create table "
						+ tableName
						+ " (newsid int, visit_timestamp bigint, visit_time string) row format delimited fields terminated by '\t'");
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/root/oldestvisit.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
	}

	public static void importOneday() throws SQLException {
		Connection connection = getConnection();
		Statement stmt = connection.createStatement();
		String tableName = "oneday";
		stmt.executeQuery("drop table if exists " + tableName);
		ResultSet res = stmt
				.executeQuery("create table "
						+ tableName
						+ " (userid int, newsid int) row format delimited fields terminated by '\t'");
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/root/oneday.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
	}

	public static void importNewsCity() throws SQLException {
		Connection connection = getConnection();
		Statement stmt = connection.createStatement();
		String tableName = "newscity";
		stmt.executeQuery("drop table if exists " + tableName);
		ResultSet res = stmt
				.executeQuery("create table "
						+ tableName
						+ " (newsid int, cities string) row format delimited fields terminated by '\t'");
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/root/newscity.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);

	}

	public static void importIntervalData() throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection(
				"jdbc:hive://192.168.1.103:10003/default", "hive", "");
		Statement stmt = con.createStatement();
		String tableName = "interval";
		stmt.executeQuery("drop table if exists " + tableName);
		ResultSet res = stmt
				.executeQuery("create table "
						+ tableName
						+ " (userid int, newsid int, interval int) row format delimited fields terminated by '\t'");
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/root/intervaldata.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
	}

	public static void main(String[] args) {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

		try {
			importIntervalData();
			importNewsData();
			importOldestVisit();
			importOneday();
			importNewsCity();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
