package edu.uchicago.SSSexecutor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSexecutor {

	final static Logger logger = LoggerFactory.getLogger(SSSexecutor.class);

	public static void main(String[] args) {
		logger.info("starting up ...");
		
		try { // write process number so shell can restart it if it crashes.
			FileWriter fstream = new FileWriter("/tmp/.SSSexecutor.proc");
			BufferedWriter out = new BufferedWriter(fstream);
			String[] pr=ManagementFactory.getRuntimeMXBean().getName().split("@");
			out.write(pr[0]+"\n");
			out.close();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
		
		Receiver r = new Receiver();
		r.connect();
		
		while (true) {
			logger.info("----------------");
			Task task = r.getJob();
			if (task.id > 0 ) {
				task.print();
				CondorSubmitter s = new CondorSubmitter();
				s.submit(task);
			} 
			else {
				try {
					Thread.currentThread().sleep(60000);
				} catch (Exception e) {
				}
			}
		}

		
	}

}
