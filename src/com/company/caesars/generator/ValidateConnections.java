package com.company.caesars.generator;

import java.sql.Connection;
import java.sql.SQLException;

public class ValidateConnections extends SQLGeneratorBase{

	public static void main(String[] args) throws SQLException {
		ValidateConnections vc = new ValidateConnections();
		vc.testConnection();
		vc.testConnectionFull1MasterDB();
		vc.testConnectionFull1TestDB();
	}
	
	private void testConnection() throws SQLException {
		Connection testCon = getConnection();
		System.out.println(testCon.toString());
		showDBUrl(testCon);
	}
	
	private void testConnectionFull1MasterDB() throws SQLException {
		Connection testCon = getConnectionFull1MasterDB();
		System.out.println(testCon.toString());
		showDBUrl(testCon);
	}
	
	private void testConnectionFull1TestDB() throws SQLException {
		Connection testCon = getConnectionFull1TestDB();
		System.out.println(testCon.toString());
		showDBUrl(testCon);
		
	}

	public void showDBUrl(Connection con) throws SQLException {
		System.out.println(con.getMetaData().getURL());
	}
}
 