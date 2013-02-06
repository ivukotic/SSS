package edu.uchicago.SSSexecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorSubmitter implements Submitter {

	final Logger logger = LoggerFactory.getLogger(CondorSubmitter.class);

	public void submit(Task task) {

		task.createFiles();

		logger.info("creating condor submission script");
		String fn = "SSS_" + task.id + ".";

		try { // script to execute
			FileWriter fstream = new FileWriter(fn + "submit");
			BufferedWriter out = new BufferedWriter(fstream);
			String res = "getenv   = False\n";
			res += "executable     = " + fn + "sh\n";
			res += "output         = " + fn + "out\n";
			res += "error          = " + fn + "error\n";
			res += "log            = " + fn + "log\n";
			res += "transfer_input_files = filter-and-merge-d3pd.py, /tmp/x509up_u20074\n";

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
			logger.info("submit output:\n");
			String s;
			while ((s = stdInput.readLine()) != null) {
				logger.info(s);
			}

			// read any errors from the submit command
			while ((s = stdError.readLine()) != null) {
				logger.error("there is an error from submit command...");
				logger.error(s);
			}
			
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

	}
}
