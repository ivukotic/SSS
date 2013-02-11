package edu.uchicago.SSSexecutor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Receiver {

	final Logger logger = LoggerFactory.getLogger(Receiver.class);

	private Connection conn;
	private final String dbhost = "intr1-v.cern.ch:10121/intr.cern.ch";

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

	}


	public Task getJob() {
		logger.debug("getJob started");
		Task task = new Task();
		try {

			// this makes sure that all the jobs are split in tasks
			CallableStatement cs = conn.prepareCall("{call SSS_SET_TASK()}");
			cs.execute();
			cs.close();

			logger.debug("Tasks were set");
			CallableStatement cs1 = conn.prepareCall("{call SSS_GET_TASK(?,?,?,?,?,?,?)}");
			cs1.registerOutParameter(1, Types.INTEGER);
			cs1.registerOutParameter(2, Types.VARCHAR);
			cs1.registerOutParameter(3, Types.VARCHAR);
			cs1.registerOutParameter(4, Types.VARCHAR);
			cs1.registerOutParameter(5, Types.VARCHAR);
			cs1.registerOutParameter(6, Types.VARCHAR);
			cs1.registerOutParameter(7, Types.VARCHAR);

			cs1.executeQuery();
			task.id = cs1.getInt(1);
			task.outFile = cs1.getString(2);
			task.branches = cs1.getString(3);
			task.cut = cs1.getString(4);
			task.tree = cs1.getString(5);
			task.treesToCopy = cs1.getString(6);
			task.deliverTo = cs1.getString(7);

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT name from SSS_FILES where taskid=" + task.id.toString());
			while (rs.next()) {
				task.inputFiles.add(rs.getString(1));
			}

		} catch (Exception e) {
			logger.error("in GetJob:");
			logger.error(e.getMessage());
		}

		logger.debug("getJob done");
		return task;
	}
	
	public Task getTaskToUpload() {
		logger.debug("getTaskToUpload started");
		Task task = new Task();
		try {

			CallableStatement cs1 = conn.prepareCall("{call SSS_GET_TASK_TO_UPLOAD(?,?,?,?)}");
			cs1.registerOutParameter(1, Types.INTEGER);
			cs1.registerOutParameter(2, Types.VARCHAR);
			cs1.registerOutParameter(3, Types.VARCHAR);
			cs1.registerOutParameter(4, Types.VARCHAR);
			cs1.executeQuery();
			task.id = cs1.getInt(1);
			task.outFile = cs1.getString(2);
			task.dataset = cs1.getString(3);
			task.deliverTo = cs1.getString(4);
			cs1.close();

		} catch (Exception e) {
			logger.error("in getTaskToUpload:");
			logger.error(e.getMessage());
		}

		logger.debug("getTaskToUpload done");
		return task;
	}

	public void setPutStatus(Integer size, Integer taskid){
		logger.debug("updating dq2-put size");

		String updateString="UPDATE SSS_SUBJOBS SET status=6, OUTPUTSIZE="+size.toString()+" WHERE taskid="+taskid.toString();
		try {
			PreparedStatement updatePut = conn.prepareStatement(updateString);
			updatePut.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
	}
}
