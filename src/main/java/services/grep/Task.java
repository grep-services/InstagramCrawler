package main.java.services.grep;

import java.util.List;

import main.java.services.grep.Account.AccountCallback;

import org.apache.commons.lang3.Range;
import org.jinstagram.entity.users.feed.MediaFeedData;

/**
 * 
 * query를 동시에 날리는 구조를 만드는게 목적이다.
 * 
 * 원래는 reservation 구조로 하려했으나, 복잡성이 꽤나 있어서
 * 그냥 main이 일 다된 task에게 query양 남은 account를 배정해주는 식으로 가기로 했다.
 * 
 * @author marine1079
 * @since 151025
 *
 */

public class Task extends Thread implements AccountCallback {

	public enum Status {// 단순히 boolean으로는 정확히 다룰 수가 없고, 그러면 꼬일 수가 있어서 이렇게 간다.
		UNAVAILABLE, WORKING, DONE;
	}
	public Status status;
	
	int id;
	String tag;
	Account account;
	Range<Long> range;
	TaskCallback callback;
	
	public Task(String tag, Range<Long> range, TaskCallback callback) {
		setDaemon(true);
		
		this.tag = tag;
		this.range = range;
		this.callback = callback;
		
		status = Status.UNAVAILABLE;
	}
	
	// constructor 이외에도 set 될 일들 많다.
	public void setAccount(Account account) {
		if(this.account != null) {// 이미 쓰던게 있었다면
			this.account.updateStatus();// status 정리해준다.
		}
		
		this.account = account;
		
		account.setCallback(this);
		account.setStatus(Account.Status.WORKING);// 단순하게, 할당된 시점부터를 working이라 잡는다.
	}
	
	public Account getAccount() {
		return account;
	}

	@Override
	public void run() {
		Logger.printMessage("<Task %d> Running", id);
		
		while(status != Status.DONE) {
			while(status == Status.WORKING) {// account, range 등이 exception 등에 의해 변경될 수 있다. 그 때 다시 working으로 돌리면서 진입한다.
				//TODO: filtering하다가 exception 난 것까지는 어떻게 할 수가 없다. 그것은 그냥 crawling 몇 번 한 평균으로서 그냥 보증한다.
				List<MediaFeedData> list = account.getListFromTag_(tag, range.getMinimum(), range.getMaximum());
			}
		}
	}
	
	@Override
	public void onAccountDone(List<MediaFeedData> list) {
		callback.onTaskDone(this, list);
	}

	@Override
	public void onAccountStop(List<MediaFeedData> list) {
		callback.onTaskStop(this, list);
	}
	
	@Override
	public void onAccountSplit(List<MediaFeedData> list, Task task) {
		callback.onTaskSplit(this, list, task);
	}

	public void splitTask(Task task) {
		account.interrupt(task);// 멈추게 되고, 결국 exception 내면서 callback할 것이다.
	}
	
	public void resizeTask(long from, long to) {
		setRange(Range.between(from, to));
	}
	
	public void startTask() {
		Logger.printMessage("<Task %d> Started", id);
		
		try {
			status = Status.WORKING;
			
			start();
		} catch(IllegalThreadStateException e) {
			Logger.printException(e);
		}
	}
	
	public void stopTask() {
		status = Status.DONE;
	}
	
	public void pauseTask() {
		status = Status.UNAVAILABLE;
	}
	
	public void resumeTask() {
		status = Status.WORKING;
	}
	
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public void setRange(Range<Long> range) {
		this.range = range;
	}
	
	public Range<Long> getRange() {
		return range;
	}
	
	public void setTaskId(int id) {
		this.id = id;
	}
	
	public int getTaskId() {
		return id;
	}
	
	public TaskCallback getCallback() {
		return callback;
	}

	public interface TaskCallback {
		void onTaskDone(Task task, List<MediaFeedData> list);
		void onTaskStop(Task task, List<MediaFeedData> list);
		void onTaskSplit(Task task, List<MediaFeedData> list, Task task_);
	}
	
}
