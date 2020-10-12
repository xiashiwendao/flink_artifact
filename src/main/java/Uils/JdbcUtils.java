package Uils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils {

//通过上面的工具就可以获取到properties文件中的键值从而可以加载驱动 获取链接 从而 可以增删改查
	private static Connection conn = null;

	public static Connection getConn() {
		PropertiesUtil.loadFile("jdbc.properties");
		String driver = "com.mysql.jdbc.Driver"; //PropertiesUtil.getPropertyValue("driver");
		String url = "jdbc:mysql://172.16.103.90:3306/url_sla?useUnicode=true&characterEncoding=utf-8&useSSL=false"; //PropertiesUtil.getPropertyValue("url");
		String username = "root"; //PropertiesUtil.getPropertyValue("username");
		String password = "root"; //PropertiesUtil.getPropertyValue("password");

		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, username, password);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
			close();
		}
		return conn;
	}

	public static void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 从数据库中获取需要统计的网站的网址
	 * 
	 * @return
	 */
	public static List<String> getUrlAndSLA() {
		//SQL语句  
		String sql = "select url, sla from tbl_url_sla";
		Connection conn = JdbcUtils.getConn();
		Statement stmt = null;
		ResultSet ret = null;
		List<String> urls = new ArrayList<String>();
		try {
			stmt = conn.createStatement();
			//执行语句，得到结果集  
			ret = stmt.executeQuery(sql);
			while (ret.next()) {
				urls.add(ret.getString(1));
			}
			ret.close();
			conn.close();// 关闭连接
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return urls;
	}
	
	public static void main(String[] args) {
		List<String> lst = JdbcUtils.getUrlAndSLA();
		for(String url : lst) {
			System.out.println("Url is: "+ url);
		}
	}
}
