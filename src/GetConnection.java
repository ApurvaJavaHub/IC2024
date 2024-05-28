import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.Scanner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azul.crs.client.models.VMArtifact.Type;
import com.ibm.db2.jcc.DB2Types;

public class GetConnection {

	private static final Logger LOGGER = LogManager.getLogger(STORED_PROCEDURE_REMASTERED.class);

	public CallableStatement connectToDB(String driver, String url, String username, String password, String query)
			throws SQLException {

		System.out.println("url: " + url);
		System.out.println("username: " + username);
		System.out.println("password: " + password);
		System.out.println("query: " + query);

		CallableStatement cstmt = null;
		Connection connection = null;
		Statement statement = null;
		PreparedStatement ps = null;
		
		String connector = getConnector(driver);
		String schemaQuery = getQueryForSchema(connector);

		try {
			Class.forName(driver);
			LOGGER.log(Level.INFO, "Driver loaded: " + driver);
			connection = DriverManager.getConnection(url, username, password);
			
			try(Statement stmt = connection.createStatement();) {
				boolean test = stmt.execute(schemaQuery);
				if (test) {
					System.out.println("################Connected#####################");
					LOGGER.log(Level.INFO, "Connected...");
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
			statement = connection.createStatement();
//	      
			LOGGER.log(Level.INFO, "Query: " + query);
//	     
			cstmt = connection.prepareCall(query);
//	      
			int inCount = 0;
			int outCount = 0;
			for (int k = 1; k <= cstmt.getParameterMetaData().getParameterCount(); k++) {
				if (cstmt.getParameterMetaData().getParameterMode(k) == 1) {
					inCount++;
				} else if (cstmt.getParameterMetaData().getParameterMode(k) == 4) {
					outCount++;
				}
			}

			System.out.println("Total parameters: " + cstmt.getParameterMetaData().getParameterCount() + "\n"
					+ "Input parameters: " + inCount + "\n" + "Output parameters: " + outCount);
			LOGGER.log(Level.INFO, "Total parameters: " + cstmt.getParameterMetaData().getParameterCount());
			
			
			if("Oracle".equalsIgnoreCase(getConnector(driver))) {
				try{
				cstmt = connection.prepareCall("{call dbms_output.enable }");
					cstmt.execute();
				}catch(Exception e) {
					e.printStackTrace();
				}
				
				ps = connection.prepareStatement(query);
				ps.execute();
			}
			
			if("Oracle".equalsIgnoreCase(getConnector(driver))) {
				try {
					cstmt = connection.prepareCall("{call dbms_output.get_line(:1,:2)}");
					cstmt.registerOutParameter(1, java.sql.Types.VARCHAR);
					cstmt.registerOutParameter(2, java.sql.Types.NUMERIC);
				}catch(Exception e) {
					e.printStackTrace();
				}
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOGGER.log(Level.ERROR, "Exception: " + e);
		} catch (ClassNotFoundException cnfex) {
			System.out.println("Problem in" + " loading or registering "+connector+" driver");
			LOGGER.log(Level.ERROR, "Exception: " + cnfex);
			cnfex.printStackTrace();
		} 
		return cstmt;
	}

	public void registerInOutParam(CallableStatement cstmt, String driver) {

		Scanner sc = new Scanner(System.in);
		String connector = getConnector(driver);
		try {
			for (int i = 1; i <= cstmt.getParameterMetaData().getParameterCount(); i++) {
//	       
				if (cstmt.getParameterMetaData().getParameterMode(i) == 1) {
					// System.out.println("---------"+cstmt.getParameterMetaData().getParameterMode(i));
					System.out.println("Enter input parameter: ");
					cstmt.setObject(i, sc.next());
					LOGGER.log(Level.INFO, "registered input-- " + i);
				} else if (cstmt.getParameterMetaData().getParameterMode(i) == 4) {
					// System.out.println("---------"+cstmt.getParameterMetaData().getParameterMode(i));
//					if("DB2".equalsIgnoreCase(connector)) {
//						cstmt.registerOutParameter(i, DB2Types.CURSOR);
//					}
					switch (connector) {
					case "DB2":
						cstmt.registerOutParameter(i, DB2Types.CURSOR);
						break;
					case "Snowflake":
						cstmt.registerOutParameter(i, Types.JAVA_OBJECT);
						break;
					}
					LOGGER.log(Level.INFO, "registered output-- " + i);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.log(Level.ERROR, "Exception: " + e);
		} finally {
			sc.close();
		}

	}

	public void getData(CallableStatement cstmt, ResultSet rs, ResultSetMetaData rsmd, Class<?> cls, String output) throws SQLException {

      int columnCount = 0;
      try {
   	    rsmd = rs.getMetaData();
   	    columnCount = rsmd.getColumnCount();
   	    String columnName ="";
           for (int i=0;i<columnCount;i++)
           {
               columnName += rsmd.getColumnName(i+1)+"  |  ";
           }

           System.out.println("/----------------------------------------------------------------------------------------------------------------------------------------------------------------/");

           System.out.println(columnName);

           System.out.println("/----------------------------------------------------------------------------------------------------------------------------------------------------------------/");
           
      }catch(Exception e) {
   	   System.out.println("No resultSet found");
      }
     
      if(rs != null) {
   	   long recordCount = 0;
   	   while(rs.next()) {
//   		   for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
//              {
                  System.out.print(rs.getString("CUSTOMERID")+"\t");
                  System.out.print(rs.getString("ACCTNUMBER")+"\t");
//              }
   		   recordCount++;
              System.out.println();
          }
      }
      System.out.println("-----------------------------------------------------------------------------------------------------------------------");

		for (int j = 1; j <= cstmt.getParameterMetaData().getParameterCount(); j++) {

			if (cstmt.getParameterMetaData().getParameterMode(j) != 1) {
				if (cls.equals(cstmt.getObject(j).getClass()) || "All".equalsIgnoreCase(output)) {
					System.out.println("-------" + cstmt.getObject(j));
				}
			}
		}
	}
	
	public Properties readPropertiesFile(String fileName) throws IOException {

		FileInputStream fis = null;
		Properties prop = null;
		try {
			fis = new FileInputStream(fileName);
			prop = new Properties();
			prop.load(fis);
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			fis.close();
		}
		return prop;
	}
	
	
	public String getConnector(String driver) {
		String connector = "";
		
//		if("com.ibm.db2.jcc.DB2Driver".equalsIgnoreCase(driver)) {
//			connector = "DB2";
//		}
		switch (driver) {
		case "com.ibm.db2.jcc.DB2Driver":
			connector = "DB2";
			break;
		case "net.snowflake.client.jdbc.SnowflakeDriver":
			connector = "Snowflake";
			break;
		case "oracle.jdbc.driver.OracleDriver":
			connector = "Oracle";
			break;
		}
		return connector;
		
	}
	
	public String getQueryForSchema(String connector) {
		
		String schemaQuery = "";
		
		switch (connector) {
		case "DB2":
			schemaQuery = "select SCHEMANAME  from syscat.SCHEMATA";
			break;
		case "Snowflake":
			schemaQuery = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES";
			break;
		case "Oracle":
			schemaQuery = "SELECT * FROM SYS_TABLES";
			break;
		}
		return schemaQuery;
	}

}
