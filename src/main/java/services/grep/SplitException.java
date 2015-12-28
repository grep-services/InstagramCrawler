package main.java.services.grep;

public class SplitException extends Exception {
	private static final long serialVersionUID = 2484495357182303170L;
	
	private static final String MSG = "Split and share with task %d";

	private Task task;
	
	public SplitException(Task task) {
		super(String.format(MSG, task.getId()));
		
		this.task = task;
	}
	
	public Task getTask() {
		return task;
	}
}
