package edu.uchicago.SSSexecutor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;

import org.apache.axis2.AxisFault;
import condor.CondorScheddStub;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class SSSexecutor {

	final private static Logger logger = Logger.getLogger(SSSexecutor.class);

	SSSexecutor(){
		PropertyConfigurator.configure(SSSexecutor.class.getClassLoader().getResource("log4j.properties"));
	}
	
	private void start(){
		logger.info("starting up ...");
		logger.info("making schedduler");
		

		try {
			CondorScheddStub sch=new CondorScheddStub();
		} catch (AxisFault e) {
			logger.warn("could not make schedduler");
			e.printStackTrace();
		}
		
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
			if (getNumberOfIdleJobs() < 2) {

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
		
		SSSexecutor SSSe=new SSSexecutor();
		SSSe.start();
		

	}

}
