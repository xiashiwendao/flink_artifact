package Uils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class MySQL_Connector {
	public static void main(String[] args) {
		List<String> lst = JdbcUtils.getUrlAndSLA();
		for(String url : lst) {
			System.out.println("Url is: "+ url);
		}
	}

}
