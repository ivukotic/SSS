package edu.uchicago.SSSexecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class CondorSubmitter implements Submitter {

	final private static Logger logger = Logger.getLogger(CondorSubmitter.class);

	public Integer submit(Task task) {

		task.createFiles();

		logger.info("creating condor submission script");
		String fn = "SSS_" +task.jID.toString()+"_"+task.id + ".";

		try { // script to execute
			FileWriter fstream = new FileWriter(fn + "submit");
			BufferedWriter out = new BufferedWriter(fstream);
			String res = "getenv   = False\n";
			res += "executable     = " + fn + "sh\n";
			res += "output         = " + fn + "out\n";
			res += "error          = " + fn + "error\n";
			res += "log            = " + fn + "log\n";
			String filesToTransfer = "filter-and-merge-d3pd.py,.OracleAccess.txt,.rootrc,";
			filesToTransfer += "/tmp/x509up_u20074,";
			filesToTransfer += fn+"inputFileList,";
			filesToTransfer += fn+"branchesList";
			if (task.cut != null)  filesToTransfer += ","+fn+"cutCode";
			res += "transfer_input_files = "+filesToTransfer+"\n";

			String value = System.getenv("SSS_UNIVERSE");
			if (value == null) {
				logger.warn("no universe defined. Please export SSS_UNIVERSE");
				logger.warn("will use VANILLA instead.");
				res += "universe       = vanilla\n";
			} else {
				res += "universe       = " + value + "\n";
			}

			value = System.getenv("SSS_REQUIREMENTS");
			if (value == null) {
				logger.warn("no Requirements defined. If needed please set variable SSS_REQUIREMENTS");
			} else {
				res += "Requirements =  " + value + "\n";
			}
			
			value = System.getenv("SSS_EXTRA_CONDOR_SETTINGS");
			if (value == null) {
				logger.warn("no SSS_EXTRA_CONDOR_SETTINGS defined. If needed please set variable SSS_EXTRA_CONDOR_SETTINGS");
			} else {
				res +=  value + "\n";
			}
			
			res += "Queue  1";

			out.write(res);
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		// submit it
		Runtime rt = Runtime.getRuntime();
		String[] comm = { "condor_submit", fn + "submit" };
		try {
			Process proc = rt.exec(comm);

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// read the output from the submit command
			logger.info("submit output:");
			String s;
			while ((s = stdInput.readLine()) != null) {
				logger.info(s);
				if (s.indexOf("submitted to cluster ")>0){
					s=s.replace(".","");
					return Integer.parseInt(s.substring(s.indexOf("cluster ")+8));
				}
			}

			// read any errors from the submit command
			while ((s = stdError.readLine()) != null) {
				logger.error("there is an error from submit command...");
				logger.error(s);
			}
			
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		return 0;
	}

	@Override
	public void kill(Task task) {
		logger.info("killing task...");
		task.print();
		Runtime rt = Runtime.getRuntime();
		String[] comm = { "condor_rm", task.queueID.toString() };
		try {
			Process proc = rt.exec(comm);

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// read the output from the submit command
			logger.info("kill output:\n");
			String s;
			while ((s = stdInput.readLine()) != null) {
				logger.info(s);
			}

			// read any errors from the kill command
			while ((s = stdError.readLine()) != null) {
				logger.error("there is an error from kill command...");
				logger.error(s);
			}
			
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
	}
}
