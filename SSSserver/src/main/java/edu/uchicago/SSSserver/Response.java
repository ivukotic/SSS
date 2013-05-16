package edu.uchicago.SSSserver;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class Response extends Thread {

	final private static Logger logger = Logger.getLogger(Response.class);

	public String md5;
	private StringBuilder buf = new StringBuilder();
	// stage starts with 0
	// 1 - parameters set
	// 2 - started dq2-ls
	// 3 - finished dq2-ls
	// 4 - started inspecting
	// 5 - finished inspecting
	// ...
	public AtomicInteger stage = new AtomicInteger();
	public AtomicInteger openFiles = new AtomicInteger();
	
	public String[] dss;
	private String mainTree;
	HashSet<String> treesToCopy = new HashSet<String>();
	HashSet<String> branchesToKeep = new HashSet<String>();
	private String cutCode;
	private String outDS;
	private String deliverTo;
	private DataContainer DC;
	private long totsize;
	
	Response() {
		buf.setLength(0);
	}

	public void append(String s) {
		buf.append(s);
	}

	public StringBuilder getStringBuffer() {
		buf.setLength(0);
		if (totsize < 0) {
			buf.append("warning:at least one of the datasets does not exist, or has no root files.");
			return buf;
		}
		buf.append("size:" + String.valueOf(totsize) + "\n");
		
		buf.append(DC.getTreeDetails());
		
		buf.append(mainTree + "\n");
		String joinedTrees = "";
		if (treesToCopy.size() > 0) {
			for (String c : treesToCopy)
				joinedTrees += "," + c;
			joinedTrees = joinedTrees.substring(1);
		}
		buf.append(joinedTrees + "\n");

		buf.append(DC.getOutputEstimate());
		
		if (outDS != null){
			// add here check that all the files have been preprocessed.
			DC.insertJob(outDS, mainTree, treesToCopy, branchesToKeep, cutCode, deliverTo);
			logger.info("submitted");
			buf.setLength(0);
			buf.append("message:Your job has been submitted.");
		}
		else buf.append("\nOK");

		logger.info("Sending response:");
		logger.info(buf.toString());
		return buf;
	}

	public void run() {
		try {
			buf.setLength(0);
			logger.info("Getting DC size....");
			totsize = DC.getInputSize();
			logger.info("sizes of ALL input DSs have been found. ");
			
			logger.info("Updating tree info... ");
			DC.updateTrees();
			logger.info("Tree info updated. ");

			logger.info("Updating output estimates... ");
			DC.updateOutputEstimate(mainTree, treesToCopy, branchesToKeep, cutCode);
			logger.info("Output estimates updated. ");
			
		} catch (Exception e) {
			logger.error("unrecognized exception: " + e.getMessage());
		}
	}

	public void parseParameters(String[] pars) {

		logger.info("datasets xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String[] sSplit = pars[0].split("=");
		if (!sSplit[0].equals("inds"))
			return;

		logger.info("inds: " + sSplit[1]);
		dss = sSplit[1].split(",");

		logger.info("trees xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		sSplit = pars[1].split("=");
		if (!sSplit[0].equals("mainTree"))
			return;

		mainTree = "undefined";
		if (sSplit.length == 2) {
			mainTree = sSplit[1];
			logger.info("mainTree: " + mainTree);
		}

		sSplit = pars[2].split("=");
		if (!sSplit[0].equals("treesToCopy"))
			return;
		HashSet<String> treesToCopy = new HashSet<String>();
		if (sSplit.length == 1) {
			logger.info("No trees to copy selected.");
		} else {
			String[] ttC = sSplit[1].split(",");
			for (String c : ttC)
				treesToCopy.add(c);
			logger.info("treesToCopy: " + sSplit[1]);
		}

		logger.info("branches xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		sSplit = pars[3].split("=");
		if (!sSplit[0].equals("branchesToKeep"))
			return;
		if (sSplit.length == 1) {
			logger.info("No branches to keep selected.");
		} else {
			String[] byComma = sSplit[1].split(",");
			for (String s : byComma) {
				String[] byNewLine = s.split("\n");
				for (String n : byNewLine)
					branchesToKeep.add(n);
			}
			logger.info("branches to keep: " + sSplit[1]);
		}

		logger.info("cut code xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		if (!pars[4].startsWith("cutCode="))
			return;
		cutCode = pars[4].substring(8);
		logger.info("cut code: " + cutCode);

		sSplit = pars[5].split("=");
		if (!sSplit[0].equals("sReq"))
			return;

		if (sSplit[1].equals("1")) {
			logger.info("submit request xxxxxxxxxxxxxxxxxxxxxxxxxx");

			sSplit = pars[6].split("=");
			if (!sSplit[0].equals("outDS"))
				return;

			if (sSplit.length == 1) {
				logger.error("No outDS !");
			} else {
				outDS = sSplit[1];
				logger.info("outDS: " + outDS);
			}

			sSplit = pars[7].split("=");
			if (!sSplit[0].equals("deliverTo"))
				return;

			if (sSplit.length == 1) {
				logger.info("No delivery");
			} else {
				deliverTo = sSplit[1];
				logger.info("deliverTo: " + deliverTo);
			}
		}
		// ==================================================
		stage.set(1);

	}

	public void setDC(DataContainer dc) {
		DC = dc;
		logger.info("DataContainer in place.");
	}

}
