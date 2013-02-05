package edu.uchicago.SSSexecutor;

public class CondorSubmitter implements Submitter {

	public void submit(Task task) {
		task.createFiles();

	}

}
