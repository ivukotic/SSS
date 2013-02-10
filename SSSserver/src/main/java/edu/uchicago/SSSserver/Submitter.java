package edu.uchicago.SSSserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Submitter {

	final Logger logger = LoggerFactory.getLogger(Submitter.class);

	private Connection conn;
	private final String dbhost = "intr1-v.cern.ch:10121/intr.cern.ch";
	private final String dbusername = "ATLAS_WANHCTEST";
	private final String dbpass = "wanhctest1";

	Submitter() {
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
	
	public void disconnect(){
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException logOrIgnore) {
			}
	}
	
	public Integer insertJob(String indataset, String outdataset, String branches, String cut, String tree, String treestokeep, String deliverTo, long inpEvents,
			long inpSize, long estSize, long estEvents) {
		Integer jobID=0;
		PreparedStatement statement = null;
		try {
			String SQL_INSERT = "INSERT INTO SSS_JOBS (OUTDATASET,BRANCHES,CUT,TREE,TREESTOKEEP,INDATASETS,DELIVERTO,INPUTEVENTS,INPUTSIZE,OUTPUTSIZE,OUTPUTEVENTS) values (?,?,?,?,?,?,?,?,?,?,?)";
			statement = conn.prepareStatement(SQL_INSERT);
			statement.setString(1, outdataset);
			statement.setString(2, branches);
			statement.setString(3, cut);
			statement.setString(4, tree);
			statement.setString(5, treestokeep);
			statement.setString(6, indataset);
			statement.setString(7, deliverTo);
			statement.setLong(8, inpEvents);
			statement.setLong(9, inpSize);
			statement.setLong(10, estSize);
			statement.setLong(11, estEvents);
			statement.executeUpdate();

			Statement getAutoGenValueStatement = conn.createStatement();
			ResultSet rs = getAutoGenValueStatement.executeQuery("select SSS_JOBS_SEQ.currval from dual");
			if (rs.next()) {
				jobID=rs.getInt(1);
				logger.info("inserted jobid: " + jobID);
			}
			rs.close();
			getAutoGenValueStatement.close();

		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			if (statement != null)
				try {
					statement.close();
				} catch (SQLException logOrIgnore) {
				}
		}
		return jobID;
	}
	
	public void insertFile(Integer jobID, String fname, Long size, Long events){
		PreparedStatement statement = null;
		try {
			String SQL_INSERT = "INSERT INTO SSS_FILES (JOBID, NAME, FILESIZE, EVENTS) values (?,?,?,?)";
			statement = conn.prepareStatement(SQL_INSERT);
			statement.setInt(1, jobID);
			statement.setString(2, fname);
			statement.setLong(3, size);
			statement.setLong(4, events);
			statement.executeUpdate();

		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			if (statement != null)
				try {
					statement.close();
				} catch (SQLException logOrIgnore) {
				}
		}
		
		// now setting status to 0 so job is taken by SSSexecutor - initially trigger sets status to -1
		try {
			String SQL_UPDATE_STATUS = "update SSS_JOBS set status=0 where jobid=?";
			statement = conn.prepareStatement(SQL_UPDATE_STATUS);
			statement.setInt(1, jobID);
			statement.executeUpdate();

		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			if (statement != null)
				try {
					statement.close();
				} catch (SQLException logOrIgnore) {
				}
		}
		
	}
}
