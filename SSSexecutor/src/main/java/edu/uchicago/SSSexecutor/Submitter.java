package edu.uchicago.SSSexecutor;

public interface Submitter {
	public void submit(Task task);
	public void kill(Task task);
}
