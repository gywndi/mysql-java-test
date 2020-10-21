package net.gywn.bulkinsert;

import java.sql.*;
import java.util.*;

public class ExtendedInsert {
	public static long x, y;
	public static final int rowCount = 100000;
	public static final int batchSize = 500;
	public static final List<String[]> rows = new ArrayList<String[]>();
	public static final String jdbcURL = "jdbc:mysql://10.5.5.11:3306/test"
			+ "?autoReconnect=true"
			+ "&cacheServerConfiguration=true"
			+ "&useLocalSessionState=true"
			+ "&elideSetAutoCommits=true"
			+ "&connectTimeout=3000"
			+ "&socketTimeout=60000"
			+ "&useSSL=false"
			+ "&cacheCallableStmts=true"
			+ "&noAccessToProcedureBodies=true"
			+ "&characterEncoding=utf8"
			+ "&characterSetResults=utf8"
			+ "&connectionCollation=utf8_bin";
	public static final String user = "test";
	public static final String pass = "test123";

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		// =====================================
		// generate rows
		// =====================================
		begin("generate rows");
		generateRows(rowCount);
		end();
		
//
//		// =====================================
//		// autocommit true & false
//		// =====================================
//		{
//			resetTable();
//			String url = jdbcURL + "&rewriteBatchedStatements=true";
//			Connection conn = DriverManager.getConnection(url, user, pass);
//			PreparedStatement pstmt = conn.prepareStatement(insertQuery);
//			begin("insert rows");
//			for(int i = 0; i < rows.size(); i++) {
//				String[] row = rows.get(i);
//				pstmt.setString(1, row[0]);
//				pstmt.setString(2, row[1]);
//				pstmt.setString(3, row[2]);
//				pstmt.execute();
//			}
//			end();
//			pstmt.close();
//			conn.close();
//		}
//		
//		{
//			resetTable();
//			
//			String url = jdbcURL;
//			Connection conn = DriverManager.getConnection(url, user, pass);
//			conn.setAutoCommit(false);
//			
//			PreparedStatement pstmt = conn.prepareStatement(insertQuery);
//			begin("insert rows ("+batchSize+")");
//			for(int i = 0; i < rows.size(); i++) {
//				String[] row = rows.get(i);
//				pstmt.setString(1, row[0]);
//				pstmt.setString(2, row[1]);
//				pstmt.setString(3, row[2]);
//				pstmt.execute();
//				if(i % batchSize == 0) {
//					conn.commit();
//				}
//			}
//			conn.commit();
//			end();
//			pstmt.close();
//			conn.close();
//		}
//
//		// =====================================
//		// Batch insert
//		// =====================================
//		{
//			resetTable();
//			String url = jdbcURL;
//			Connection conn = DriverManager.getConnection(url, user, pass);
//			conn.setAutoCommit(false);
//			
//			PreparedStatement pstmt = conn.prepareStatement(insertQuery);
//			begin("insert batch rows ("+batchSize+")");
//			for(int i = 0; i < rows.size(); i++) {
//				String[] row = rows.get(i);
//				pstmt.setString(1, row[0]);
//				pstmt.setString(2, row[1]);
//				pstmt.setString(3, row[2]);
//				pstmt.addBatch();
//				pstmt.clearParameters();
//				if(i % batchSize == 0) {
//					pstmt.executeBatch();
//					conn.commit();
//				}
//			}
//			pstmt.executeBatch();
//			conn.commit();
//			end();
//			pstmt.close();
//			conn.close();
//		}
//
//		// =====================================
//		// Batch insert(extended)
//		// =====================================
//		{
//			resetTable();
//			String url = jdbcURL + "&rewriteBatchedStatements=true";
//			Connection conn = DriverManager.getConnection(url, user, pass);
//			
//			PreparedStatement pstmt = conn.prepareStatement(insertQuery);
//			begin("insert batch rows ("+batchSize+")");
//			for(int i = 0; i < rows.size(); i++) {
//				String[] row = rows.get(i);
//				pstmt.setString(1, row[0]);
//				pstmt.setString(2, row[1]);
//				pstmt.setString(3, row[2]);
//				pstmt.addBatch();
//				pstmt.clearParameters();
//				if(i % batchSize == 0) {
//					pstmt.executeBatch();
//				}
//			}
//			pstmt.executeBatch();
//			end();
//			pstmt.close();
//			conn.close();
//		}

		// =====================================
		// Batch insert(extended) with 5 threads
		// =====================================
		{
			final int chunkSize = rowCount / 10; 
			resetTable();
			for(int i = 0; i < rows.size(); i+=chunkSize) {
				final int pos = i; 
				new Thread(new Runnable() {
					public void run() {
						int s = pos;
						int e = pos+chunkSize;
						
						executeBatch("thd-" + e, s, e);
					}
				}).start();
			}
		}
	}
	
	public static void executeBatch(String name, int start, int end){
		try {
			String url = jdbcURL + "&rewriteBatchedStatements=true";
			Connection conn = DriverManager.getConnection(url, user, pass);

			PreparedStatement pstmt = conn.prepareStatement(insertQuery);
			begin("[" + name + "] insert batch rows (" + batchSize + ")");
			for (int i = start; i < end; i++) {
				String[] row = rows.get(i);
				pstmt.setString(1, row[0]);
				pstmt.setString(2, row[1]);
				pstmt.setString(3, row[2]);
				pstmt.addBatch();
				pstmt.clearParameters();
				if (i % batchSize == 0) {
					pstmt.executeBatch();
				}
			}
			pstmt.executeBatch();
			end(name);
			pstmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(name + "::" + e);
		}
	}
	
	public static void resetTable() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection(jdbcURL, user, pass);
		Statement stmt = conn.createStatement();
		stmt.execute("drop table if exists dummy");
		stmt.execute("create table dummy("
			+ " seq int not null primary key auto_increment,"
			+ " c1 bigint not null,"
			+ " c2 varchar(64) not null,"
			+ " c3 bigint not null,"
			+ " key ix_c1(c1),"
			+ " key ix_c3(c3)"
			+ ")");
		stmt.close();
		conn.close();
	}

	public static void generateRows(int size) {
		Random RAND = new Random();
		for (int i = 0; i < size; i++) {
			String[] row = { Long.toString(Math.abs(RAND.nextLong())), dummy,
					Long.toString(System.nanoTime()), };
			rows.add(row);
		}
	}

	public static void begin(String message) {
		System.out.println("===========================");
		System.out.printf("%s \n", message);
		x = System.nanoTime();
	}

	public static void end() {
		long g = System.nanoTime() - x;
		System.out.printf(">> %15.3f ms\n", (double) g / 1000000);
	}

	public static void end(String name) {
		long g = System.nanoTime() - x;
		System.out.printf(">> [%s] %15.3f ms\n", name, (double) g / 1000000);
	}

	public static final String dummy = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";


	public static final String trucateQuery = "truncate table dummy";
	public static final String insertQuery = "insert into dummy (c1,c2,c3) values (?,?,?)";
}
