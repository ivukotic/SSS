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
			cs = conn.prepareCall("{call SSS_SET_TASK()}");
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
			CallableStatement cs1 = conn.prepareCall("{call SSS_GET_TASK(?,?,?,?,?,?,?,?)}");
			cs1.registerOutParameter(1, Types.INTEGER);
			cs1.registerOutParameter(2, Types.VARCHAR);
			cs1.registerOutParameter(3, Types.CLOB);
			cs1.registerOutParameter(4, Types.CLOB);
			cs1.registerOutParameter(5, Types.VARCHAR);
			cs1.registerOutParameter(6, Types.VARCHAR);
			cs1.registerOutParameter(7, Types.VARCHAR);
			cs1.registerOutParameter(8, Types.VARCHAR);

			cs1.executeQuery();
			task.id = cs1.getInt(1);
			task.outFile = cs1.getString(2);
			task.branches = cs1.getString(3);
			task.cut = cs1.getString(4);
			task.tree = cs1.getString(5);
			task.treesToCopy = cs1.getString(6);
			task.dataset = cs1.getString(7);
			task.deliverTo = cs1.getString(8);
			cs1.close();

		} catch (SQLException sqle) {
			logger.error("in getJob SSS_GET_TASK:" + sqle.getMessage());
		} catch (Exception e) {
			logger.error("in getJob SSS_GET_TASK:" + e.getMessage());
		}

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT name from SSS_FILES where taskid=" + task.id.toString());
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

}
