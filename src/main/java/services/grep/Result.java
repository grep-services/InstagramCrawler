package main.java.services.grep;

/**
 * 
 * exception이긴 하지만 result의 역할로서
 * status와 object를 담아 전달된다.
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
	private Object object;
	
	public Result(Status status, Object object) {
		super(String.format(MSG));
		
		this.status = status;
		this.object = object;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public Object getObject() {
		return object;
	}
}
