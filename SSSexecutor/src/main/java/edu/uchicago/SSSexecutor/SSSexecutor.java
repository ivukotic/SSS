package edu.uchicago.SSSexecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSexecutor {

	final static Logger logger = LoggerFactory.getLogger(SSSexecutor.class);

	public static void main(String[] args) {
		logger.info("starting up ...");

		logger.info("Setting up dq2 environment ...");

		logger.info("connecting to ORACLE server ...");

		while (true) {
			logger.info("----------------");
			Receiver r = new Receiver();
			r.connect();
			Task task = r.getJob();
			r.disconnect();
			if (task.id > 0 ) {
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
