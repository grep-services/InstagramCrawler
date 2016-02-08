package main.java.services.grep;

import java.util.List;

import org.jinstagram.entity.users.feed.MediaFeedData;

/**
 * 
 * exception이긴 하지만 result의 역할로서
 * result, message를 담아 전달된다.
 * 
 * status, list를 담기 위한 class로 변경.
 * 
 * @author marine1079
 * @since 151228
 *
 */
public class Result {
	
	/*
	 * normal은 normal result, normal exception, batch를 포함.
	 * empty는 range null.
	 * exceed는 instagram exception. limit 0는 어차피 다시 exceeded될 것이다.
	 */
	public enum Status {
		Normal, Empty, Exceed
	};
	
	private Status status;
	private List<MediaFeedData> result;
	
	public Result(Status status, List<MediaFeedData> result) {
		//super();
		
		this.status = status;
		this.result = result;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public List<MediaFeedData> getResult() {
		return result;
	}
	
}
