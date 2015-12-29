package main.java.services.grep;

import java.util.List;

import org.jinstagram.entity.users.feed.MediaFeedData;

/**
 * 
 * exception이긴 하지만 result의 역할로서
 * status와 result, task를 담아 전달된다.
 * object로 봐도 되지만, 뭐 일단 어느정도 정해져있으니 그냥 간다.
 * 
 * @author marine1079
 * @since 151228
 *
 */
public class Result extends Exception {
	private static final long serialVersionUID = 2484495357182303170L;
	
	private static final String MSG = "<Result>";
	
	public enum Status {
		NORMAL, LIMIT, SPLIT;
	}

	private Status status;
	private List<MediaFeedData> result;
	private Task task;
	
	public Result(Status status, List<MediaFeedData> result, Task task) {
		super(String.format(MSG));
		
		this.status = status;
		this.result = result;
		this.task = task;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public List<MediaFeedData> getResult() {
		return result;
	}
	
	public Task getTask() {
		return task;
	}
}
