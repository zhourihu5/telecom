package test;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
 
public class test {
 
	public static void main(String[] args) throws SQLException {
		Statement stmt = null;
		ResultSet rset = null;
		
		Connection con = DriverManager.getConnection("jdbc:phoenix:192.168.52.100:2181");
		stmt = con.createStatement();

		PreparedStatement statement = con.prepareStatement(" select TIME_INDEX from SIGNAL_STRENGTH limit 10");
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(rset.getString("TIME_INDEX"));
		}
		statement.close();
		con.close();
	}
}
