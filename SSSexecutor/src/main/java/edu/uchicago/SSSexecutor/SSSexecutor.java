package edu.uchicago.SSSexecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSexecutor {

	final static Logger logger = LoggerFactory.getLogger(SSSexecutor.class);

	private static Integer getNumberOfIdleJobs() {
		Integer idlejobs = 0;
		Runtime rt = Runtime.getRuntime();
		String[] comm = {
				"/bin/sh",
				"-c",
				"condor_q " + System.getProperty("user.name") + " | grep \" I \" | wc -l"
				};

		try {
			Process proc = rt.exec(comm);

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// read the output from the submit command
			logger.info("idle jobs: ");
			String s = stdInput.readLine();
			logger.info(s);
			idlejobs=Integer.parseInt(s);
			
			// read any errors from the submit command
			while ((s = stdError.readLine()) != null) {
				logger.error("there is an error from command lookign for idle jobs...");
				logger.error(s);
			}

		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
		return idlejobs;
	}

	public static void main(String[] args) {
		logger.info("starting up ...");

		try { // write process number so shell can restart it if it crashes.
			FileWriter fstream = new FileWriter("/tmp/.SSSexecutor.proc");
			BufferedWriter out = new BufferedWriter(fstream);
			String[] pr = ManagementFactory.getRuntimeMXBean().getName().split("@");
			out.write(pr[0] + "\n");
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		Receiver r = new Receiver();
		r.connect();

		while (true) {
			logger.info("----------------");

			// first check number of idle jobs for this user
			if (getNumberOfIdleJobs() < 2) {

				Task task = r.getJob();
				if (task.id > 0) {
					task.print();
					CondorSubmitter s = new CondorSubmitter();
					s.submit(task);
				} else {
					try {
						Thread.currentThread().sleep(60000);
					} catch (Exception e) {
					}
				}
				
			}
		}

	}

}
