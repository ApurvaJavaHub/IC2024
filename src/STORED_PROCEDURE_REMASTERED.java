import java.io.Console;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.db2.jcc.am.sc;

/*
 * Author:-Apurva Gawande
 *
 * Execute stored procedure with IN or OUT or INOUT parameters
 */

public class STORED_PROCEDURE_REMASTERED {

	private static final Logger LOGGER = LogManager.getLogger(STORED_PROCEDURE_REMASTERED.class);

	public static void main(String[] args) throws SQLException,IOException {

		// refer
//     https://www.ibm.com/docs/en/db2/9.7?topic=cspija-retrieving-data-from-cursor-output-parameters-in-jdbc-applications
//     https://www.ibm.com/docs/en/db2-for-zos/12?topic=sql-calling-stored-procedures-in-jdbc-applications

		// TODO Auto-generated method stub
		String driver = null;
		String url = null;
		String query = null;
		String output = null;
		Class<?> cls = null;
		ResultSet rs = null;
		CallableStatement cstmt = null;
		PreparedStatement ps = null;
		ResultSetMetaData rsmd = null;
		GetConnection gc = new GetConnection();
		String enteredPasswordStr = null;
		Scanner sc = new Scanner(System.in);

		try {
			Properties prop2 = gc.readPropertiesFile("application.properties");

			LOGGER.log(Level.INFO, "application.properties loaded.....");

			driver = prop2.getProperty("driver");
			url = prop2.getProperty("url");
			query = prop2.getProperty("query");
//			query = "EXECUTE IMMEDIATE $$ DECLARE select_statement varchar; res RESULTSET;"
//					+ "BEGIN"
//					+ "    select_statement := 'SELECT customerid,acctnumber,firstname,lastname,gender,birthdate,datefirstpurchase,phoneno,email,yearlyincome,addressid FROM ICEDQ_DB.\"PUBLIC\".CUSTOMER limit 10'; res := (EXECUTE IMMEDIATE :select_statement); RETURN TABLE(res);"
//					+ "END; $$";
			output = prop2.getProperty("outputInFormat");
//			System.out.println("Enter username: ");
			String username = prop2.getProperty("username");
			try {
				Console console = System.console();
				char[] enteredPassword = null;
				enteredPassword = console.readPassword("Enter password here: ");
				enteredPasswordStr = String.valueOf(enteredPassword);
			}catch(Exception e) {
				System.out.println("Enter password here: ");
				enteredPasswordStr = sc.next();
			}

			if (output.length() == 0) {
				output = "All";
			}
			if ("String".equalsIgnoreCase(output) || "VARCHAR".equalsIgnoreCase(output)) {
				cls = java.lang.String.class;
			} else if ("Integer".equalsIgnoreCase(output) || "int".equalsIgnoreCase(output)) {
				cls = java.lang.Integer.class;
			} else if ("".equalsIgnoreCase(output) || ("All".equalsIgnoreCase(output))) {
				cls = java.lang.Object.class;
			}

			cstmt = gc.connectToDB(driver, url, username, enteredPasswordStr, query);

			gc.registerInOutParam(cstmt,driver);
			

			cstmt.execute();
			rs = cstmt.getResultSet();
			
			
			if("Oracle".equalsIgnoreCase(gc.getConnector(driver))) {
				cstmt.getString(1);
			}
			gc.getData(cstmt, rs, rsmd, cls, output);

		} catch (SQLException sqlex) {
			sqlex.printStackTrace();
			LOGGER.log(Level.ERROR, "Exception------: " + sqlex);
		} catch (Exception e) {
			System.out.println("Exception \n " + e);
			LOGGER.log(Level.ERROR, "Exception=====: " + e.getMessage());
			e.printStackTrace();
		} finally {
			if(cstmt != null) {
				cstmt.close();				
			}
			sc.close();
			if (rs != null) {
				rs.close();
			}
			LOGGER.log(Level.INFO, "Connection Closed...");
		}

	}

}
