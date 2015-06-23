import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class GetTrainPositive {
	
	public static void getDistinctUserid() throws SQLException {
		Connection connection = HiveUtil.getConnection();
		Statement statement = connection.createStatement();
		String tableName = "users";
		String sql = "drop table if exists " + tableName;
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		sql = "create table " + tableName + " (userid int) row format delimited fields terminated by '\t'";
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		sql = "insert into table " + tableName + " select distinct userid from usernews";
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		/*ResultSet resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println("userid: " + resultSet.getInt(1));
		}*/
	}
	
	public static void getDistinctNewsid() throws SQLException {
		Connection connection = HiveUtil.getConnection();
		Statement statement = connection.createStatement();
		String tableName = "news";
		String sql = "drop table if exists " + tableName;
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		sql = "create table " + tableName + " (newsid int) row format delimited fields terminated by '\t'";
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		sql = "insert into table " + tableName + " select distinct newsid from usernews";
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
	}
	
	//hive真的是好慢啊，看来只能做些简单的查询或者是单次的结果验证，要快速的存储还是要用起来hbase
	public static void getTrainPositive() throws SQLException {
		Connection connection = HiveUtil.getConnection();
		Statement statement = connection.createStatement();
		String tableName = "trainpositive";
		String sql = "drop table if exists " + tableName;
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		sql = "create table " + tableName + " (userid int, newsid int, visit_timestamp bigint, visit_time string) row format delimited fields terminated by '\t'";
		System.out.println("running sql: " + sql);
		statement.executeQuery(sql);
		sql = "select userid from users";
		System.out.println("running sql: " + sql);
		ResultSet userids = statement.executeQuery(sql);
		while(userids.next()){
			int userid = userids.getInt(1);
			String getVisitTime = "select visit_timestamp from usernews where userid = " + userid;
			System.out.println("running sql: " + getVisitTime);
			ResultSet visitTimes = statement.executeQuery(getVisitTime);
			List<Long> visitTimeList = new ArrayList<Long>();
			while(visitTimes.next()){
				long visitTime = visitTimes.getLong(1);
				System.out.println("visit time: " + visitTime);
				visitTimeList.add(visitTime);
			}
			
			Collections.sort(visitTimeList);
			long targetVisitTimestamp = visitTimeList.get(visitTimeList.size()-2);
			String getFinalResult = "insert into table " + tableName + " select userid, newsid, visit_timestamp, visit_time from usernews where userid = " + userid + " and visit_timestamp = " + targetVisitTimestamp;
			System.out.println("running sql: " + getFinalResult);
			statement.executeQuery(getFinalResult);
		}
	} 
	
	public static void validateData() throws SQLException{
		Connection connection = HiveUtil.getConnection();
		Statement statement = connection.createStatement();
		String sql = "select count(*) from interval";
		System.out.println("running sql: " + sql);
		ResultSet resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
		sql = "select count(*) from newscity";
		System.out.println("running sql: " + sql);
		resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
		sql = "select count(*) from oldestvisit";
		System.out.println("running sql: " + sql);
		resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
		sql = "select count(*) from oneday";
		System.out.println("running sql: " + sql);
		resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
		sql = "select count(*) from usernews";
		System.out.println("running sql: " + sql);
		resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
		sql = "select count(*) from users";
		System.out.println("running sql: " + sql);
		resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
		sql = "select count(*) from news";
		System.out.println("running sql: " + sql);
		resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(resultSet.getInt(1));
		}
	}
	
	public static void main(String[] args) throws SQLException{
		//getDistinctUserid();
		//getDistinctNewsid();
		//getTrainPositive();
		validateData();
	}

}
