package edu.uchicago.SSSserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Submitter extends Thread {

	final Logger logger = LoggerFactory.getLogger(Submitter.class);

	private Connection conn;
	private final String dbhost = "intr1-v.cern.ch:10121/intr.cern.ch";

	private String inputdatasets;
	private String outdataset;
	private String branches;
	private String cutCode;
	private String mainTree;
	private String tToCopy;
	private String deliverTo;
	private long inpEvents;
	private long inpSize;
	private long estSize;
	private long estEvents;

	private class FileDetails {
		FileDetails(String fN, long s, long e) {
			fName = fN;
			size = s;
			events = e;
		}

		String fName;
		long size;
		long events;
	}

	private ArrayList<FileDetails> fDs;

	Submitter() {
		conn = null;
		fDs = new ArrayList<FileDetails>();
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

	public void disconnect() {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException logOrIgnore) {
			}
	}

	public void setValues(String inputdatasets, String outdataset, String branches, String cutCode, String mainTree, String tToCopy, String deliverTo,
			long inpEvents, long inpSize, long estSize, long estEvents) {
		this.inputdatasets = inputdatasets;
		this.outdataset = outdataset;
		this.branches = branches;
		this.cutCode = cutCode;
		this.mainTree = mainTree;
		this.tToCopy = tToCopy;
		this.deliverTo = deliverTo;
		this.inpEvents = inpEvents;
		this.inpSize = inpSize;
		this.estSize = estSize;
		this.estEvents = estEvents;
	}

	public void setFile(String fname, Long size, Long events) {
		fDs.add(new FileDetails(fname, size, events));
	}

	public Integer insertJob() {
		Integer jobID = 0;
		PreparedStatement statement = null;
		try {
			String SQL_INSERT = "INSERT INTO ATLAS_WANHCTEST.SSS_JOBS (OUTDATASET,CBRANCHES,CCUT,TREE,TREESTOKEEP,INDATASETS,DELIVERTO,INPUTEVENTS,INPUTSIZE,OUTPUTSIZE,OUTPUTEVENTS) values (?,?,?,?,?,?,?,?,?,?,?)";
			statement = conn.prepareStatement(SQL_INSERT);
			statement.setString(1, outdataset);
			statement.setString(2, branches);
			statement.setString(3, cutCode);
			statement.setString(4, mainTree);
			statement.setString(5, tToCopy);
			statement.setString(6, inputdatasets);
			statement.setString(7, deliverTo);
			statement.setLong(8, inpEvents);
			statement.setLong(9, inpSize);
			statement.setLong(10, estSize);
			statement.setLong(11, estEvents);
			statement.executeUpdate();

			Statement getAutoGenValueStatement = conn.createStatement();
			ResultSet rs = getAutoGenValueStatement.executeQuery("select ATLAS_WANHCTEST.SSS_JOBS_SEQ.currval from dual");
			if (rs.next()) {
				jobID = rs.getInt(1);
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

	public void insertFiles(Integer jobID) {
		for (FileDetails fd : fDs) {
			PreparedStatement statement = null;
			try {
				String SQL_INSERT = "INSERT INTO ATLAS_WANHCTEST.SSS_FILES (JOBID, NAME, FILESIZE, EVENTS) values (?,?,?,?)";
				statement = conn.prepareStatement(SQL_INSERT);
				statement.setInt(1, jobID);
				statement.setString(2, fd.fName);
				statement.setLong(3, fd.size);
				statement.setLong(4, fd.events);
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

			// now setting status to 0 so job is taken by SSSexecutor -
			// initially trigger sets status to -1
			try {
				String SQL_UPDATE_STATUS = "update ATLAS_WANHCTEST.SSS_JOBS set status=0 where jobid=?";
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

	public void run() {
		try {
			connect();
			Integer jobID = insertJob();
			if (jobID > 0) 
				insertFiles(jobID);
			disconnect();
		} catch (Exception e) {
			logger.error("unrecognized exception: " + e.getMessage());
		}
	}

}
