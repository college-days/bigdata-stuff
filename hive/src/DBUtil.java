

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil {

	private static Connection connection = null;

	public static Connection getDBConn() throws InstantiationException, IllegalAccessException {
		if (connection == null) {
			try {
				Class.forName("org.postgresql.Driver").newInstance();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			try {
				connection = DriverManager.getConnection(Config.connString, Config.connUser, Config.connPassword);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return connection;
	}
}
