package edu.uchicago.SSSexecutor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
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
	private final String dbusername = "ATLAS_WANHCTEST";
	private final String dbpass = "wanhctest1";

	Receiver() {
		conn = null;
	}

	public void connect() {
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

	public void disconnect() {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException logOrIgnore) {
			}
	}

	public Task getJob() {
		logger.info("getJob started");
		Task task = new Task();
		try {

			// this makes sure that all the jobs are split in tasks
			CallableStatement cs = conn.prepareCall("{call SSS_SET_TASK()}");
			cs.execute();
			cs.close();

			logger.info("Tasks were set");
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
			ResultSet rs = stmt.executeQuery("SELECT inputfiles from SSS_SubJobs where taskid=" + task.id.toString());
			while (rs.next()) {
				task.inputFiles.add(rs.getString(1));
			}

		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		logger.info("getJob done");
		return task;
	}

}
