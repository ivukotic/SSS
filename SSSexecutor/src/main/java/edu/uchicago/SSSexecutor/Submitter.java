package edu.uchicago.SSSexecutor;

public interface Submitter {
	public Integer submit(Task task);
	public void kill(Task task);
}
