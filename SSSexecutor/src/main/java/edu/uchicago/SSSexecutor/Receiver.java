package edu.uchicago.SSSexecutor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.log4j.Logger;

public class Receiver {

	final private static Logger logger = Logger.getLogger(Receiver.class);

	private Connection conn;
	private final String dbhost = "intr1-v.cern.ch:10121/intr.cern.ch";
	private CallableStatement cs;

	Receiver() {
		conn = null;
	}

	public void connect() {

		logger.info("connecting to ORACLE server ...");

		String dbusername = System.getenv("dbusername");
		if (dbusername == null) {
			logger.info("no dbusername defined. Please set it.");
			System.exit(1);
		}

		String dbpass = System.getenv("dbpass");
		if (dbpass == null) {
			logger.info("no dbpass defined. Please set it.");
			System.exit(1);
		}

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + dbhost, dbusername, dbpass);
		} catch (ClassNotFoundException e) {
			logger.error("Could not find the database driver!");
		} catch (SQLException e) {
			logger.error("Could not connect to the database");
			logger.error(e.getMessage());
		}

		try {
			cs = conn.prepareCall("{call ATLAS_WANHCTEST.SSS_SET_TASK()}");
		} catch (SQLException e) {
			logger.error("can't create statement SSS_SET_TASK()" + e.getMessage());
		}

	}

	public Task getJob() {
		logger.debug("getJob started");
		Task task = new Task();
		try {

			// this makes sure that all the jobs are split in tasks
			cs.execute();
			// cs.close();

			logger.debug("Tasks were set");
			CallableStatement cs1 = conn.prepareCall("{call ATLAS_WANHCTEST.SSS_GET_TASK(?,?,?,?,?,?,?,?,?)}");
			cs1.registerOutParameter(1, Types.INTEGER);
			cs1.registerOutParameter(2, Types.INTEGER);
			cs1.registerOutParameter(3, Types.VARCHAR);
			cs1.registerOutParameter(4, Types.CLOB);
			cs1.registerOutParameter(5, Types.CLOB);
			cs1.registerOutParameter(6, Types.VARCHAR);
			cs1.registerOutParameter(7, Types.VARCHAR);
			cs1.registerOutParameter(8, Types.VARCHAR);
			cs1.registerOutParameter(9, Types.VARCHAR);

			cs1.executeQuery();
			task.id = cs1.getInt(1);
			task.jID = cs1.getInt(2);
			task.outFile = cs1.getString(3);
			task.branches = cs1.getString(4);
			task.cut = cs1.getString(5);
			task.tree = cs1.getString(6);
			task.treesToCopy = cs1.getString(7);
			task.dataset = cs1.getString(8);
			task.deliverTo = cs1.getString(9);
			cs1.close();

		} catch (SQLException sqle) {
			logger.error("in getJob SSS_GET_TASK:" + sqle.getMessage());
		} catch (Exception e) {
			logger.error("in getJob SSS_GET_TASK:" + e.getMessage());
		}

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT name from ATLAS_WANHCTEST.SSS_FILES where taskid=" + task.id.toString());
			while (rs.next()) {
				task.inputFiles.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
		} catch (SQLException sqle) {
			logger.error("in getJob SELECT" + sqle.getMessage());
		} catch (Exception e) {
			logger.error("in getJob SELECT:" + e.getMessage());
		}

		logger.debug("getJob done");
		return task;
	}
	
	public void setQID(Integer tid, Integer qid){
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("UPDATE ATLAS_WANHCTEST.SSS_SUBJOBS SET queueID="+qid.toString()+" where taskid=" + tid.toString());
			rs.close();
			stmt.close();
		} catch (SQLException sqle) {
			logger.error("in UPDATE SSS_SUBJOBS SET queueID " + sqle.getMessage());
		} catch (Exception e) {
			logger.error("in UPDATE SSS_SUBJOBS SET queueID :" + e.getMessage());
		}
		
	}
	
	public Task getJobToKill(){
		logger.debug("getJobToKill ...");
		Task task = new Task();
		try {


			logger.debug("Tasks were set");
			CallableStatement cs1 = conn.prepareCall("{call ATLAS_WANHCTEST.SSS_GET_TASK_TO_KILL(?,?,?)}");
			cs1.registerOutParameter(1, Types.INTEGER);
			cs1.registerOutParameter(2, Types.INTEGER);
			cs1.registerOutParameter(3, Types.VARCHAR);

			cs1.executeQuery();
			task.id = cs1.getInt(1);
			task.queueID=cs1.getInt(2);
			task.outFile = cs1.getString(3);
			cs1.close();

		} catch (SQLException sqle) {
			logger.error("SSS_GET_TASK_TO_KILL:" + sqle.getMessage());
		} catch (Exception e) {
			logger.error("SSS_GET_TASK_TO_KILL:" + e.getMessage());
		}

		logger.debug("getJobToKill done.");
		return task;
	}
}
