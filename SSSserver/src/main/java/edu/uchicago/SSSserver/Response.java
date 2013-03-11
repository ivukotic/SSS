package edu.uchicago.SSSserver;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Response {
	
	final static Logger logger = LoggerFactory.getLogger(Response.class);
	public String md5;
	private StringBuilder buf = new StringBuilder();
	// stage starts with 0 
	// 1 - started dq2-ls
	// 2 - finished dq2-ls
	// 3 - started inspecting
	// 4 - finished inspecting
	// ...
	public AtomicInteger stage = new AtomicInteger();
	public AtomicInteger openFiles=new AtomicInteger();
	
	private String[] dss;
	private String mainTree;
	HashSet<String> treesToCopy = new HashSet<String>();
	HashSet<String> branchesToKeep = new HashSet<String>();
	private String cutCode;
	private String outDS;
	private String deliverTo;
	
	Response(){
		buf.setLength(0);
	}
	
	public void append(String s){
		buf.append(s);
	}
	
	public StringBuilder getStringBuffer(){
		return buf;
	}
	


	public void parseParameters(String[] pars){
		
		logger.info("datasets xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		String[] sSplit = pars[0].split("=");
		if (!sSplit[0].equals("inds")) return;
		
		logger.info("inds: " + sSplit[1]);
		dss=sSplit[1].split(",");
		

		logger.info("trees xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		sSplit = pars[1].split("=");
		if (!sSplit[0].equals("mainTree")) return;
		
		mainTree = null;
		if (sSplit.length == 2) {
			mainTree = sSplit[1];
			logger.info("mainTree: " + mainTree);
		} 
		
		sSplit = pars[2].split("=");
		if (!sSplit[0].equals("treesToCopy")) return;
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
		if (!sSplit[0].equals("branchesToKeep")) return;
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
		if (!pars[4].startsWith("cutCode=")) return;
		cutCode = pars[4].substring(8);
		logger.info("cut code: " + cutCode);
		
		
		sSplit = pars[5].split("=");
		if (!sSplit[0].equals("sReq")) return;

		if (sSplit[1].equals("1")) {
			logger.info("submit request xxxxxxxxxxxxxxxxxxxxxxxxxx");

			sSplit = pars[6].split("=");
			if (!sSplit[0].equals("outDS")) return;

			if (sSplit.length == 1) {
				logger.error("No outDS !");
			} else {
				outDS = sSplit[1];
				logger.info("outDS: " + outDS);
			}

			sSplit = pars[7].split("=");
			if (!sSplit[0].equals("deliverTo")) return;

			if (sSplit.length == 1) {
				logger.info("No delivery");
			} else {
				deliverTo = sSplit[1];
				logger.info("deliverTo: " + deliverTo);
			}
		}
		// ==================================================
		
		
		
	}

}
