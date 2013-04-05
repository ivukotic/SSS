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
		String[] comm = { "/bin/sh", "-c", "condor_q " + System.getProperty("user.name") + " | grep \" I \" | wc -l" };

		try {
			Process proc = rt.exec(comm);

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

			// read the output from the submit command
			String s = stdInput.readLine();
			logger.info("idle jobs: " + s);
			idlejobs = Integer.parseInt(s);

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

	public static void waitAminute() {
		try {
			logger.info("----------------");
			Thread.currentThread().sleep(30000);
		} catch (Exception e) {
		}
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

		CondorSubmitter s = new CondorSubmitter();

		while (true) {
			
			boolean wait=true;
			// first check number of idle jobs for this user
			if (getNumberOfIdleJobs() < 2000) {

				Task task = r.getJob();
				if (task.id > 0) {
					task.print();
					Integer queueID=s.submit(task);
					logger.info("job submitted with id:"+queueID.toString());
					if (queueID>0)
						r.setQID(task.id, queueID);
					wait=false;
				}

			}
			
			Task taskToKill = r.getJobToKill();
			if (taskToKill.id>0){
				s.kill(taskToKill);
				wait=false;
			} 
			
			if (wait==true)
				waitAminute();
		}

	}

}
