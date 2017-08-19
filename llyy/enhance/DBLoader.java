package llyy.enhance;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class DBLoader {

	public static void dbLoader(HashMap<String, String> ruleMap) {

		Connection conn = null;
		Statement st = null;
		ResultSet res = null;
		
		try {

			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.161.1:3306/mydb3", "root", "Wj1227");
			st = conn.createStatement();
			res = st.executeQuery("select url,info from urlrule");
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));
			}

		} catch (Exception e) {
			e.printStackTrace();
			
		} finally {
			try{
				if(res!=null){
					res.close();
				}
				if(st!=null){
					st.close();
				}
				if(conn!=null){
					conn.close();
				}

			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}
	
	
	public static void main(String[] args) {
		DBLoader db = new DBLoader();
		HashMap<String, String> map = new HashMap<String,String>();
		db.dbLoader(map);
		System.out.println(map.size());
	}

}
