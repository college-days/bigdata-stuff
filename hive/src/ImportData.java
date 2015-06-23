import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;

public class ImportData {

	public static void importInterval() {
		try {
			Connection connection = DBUtil.getDBConn();
			Statement statement = connection.createStatement();
			String sql = "select * from interval";
			System.out.println("Running: " + sql);
			ResultSet res = statement.executeQuery(sql);
			String filePath = Config.INTERVAL_DATA;
			FileWriter writer = new FileWriter(new File(filePath));
			
			while (res.next()) {
				//System.out.println(res.getString(1));
				int userid = res.getInt(2);
				int newsid = res.getInt(3);
				int interval = res.getInt(4);
				System.out.println("userid: " + userid + " newsid: " + newsid + " interval: " + interval);
				String result = userid + "\t" + newsid + "\t" + interval + "\n";
				writer.write(result);
			}
			writer.flush();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void importNews(){
		try {
			Connection connection = DBUtil.getDBConn();
			Statement statement = connection.createStatement();
			String sql = "select * from news";
			System.out.println("Running: " + sql);
			ResultSet res = statement.executeQuery(sql);
			String filePath = Config.INTERVAL_DATA;
			//FileWriter writer = new FileWriter(new File(filePath));
			
			while (res.next()) {
				//System.out.println(res.getString(1));
				int userid = res.getInt(2);
				int newsid = res.getInt(3);
				Time visit_time = res.getTime(4);
				long visit_timelong = visit_time.getTime();
				long visit_timestamp = res.getDate(4).getTime();
				System.out.println("userid: " + userid + " newsid: " + newsid + " interval: " +  visit_time + " long: " + visit_timelong);
				//String result = userid + "\t" + newsid + "\t" + visit_timestamp + "\n";
				//writer.write(result);
			}
			//writer.flush();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		importInterval();
		//importNews();
	}

}
