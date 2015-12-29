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

public class Task extends Thread {

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
	public void setAccount(Account account) {/*
		if(this.account != null) {// 이미 쓰던게 있었다면
			this.account.updateStatus();// status 정리해준다.
		}*///일단 이 부분은, account set 자체가 기존 account refresh를 수반하기에 필요없다고 생각한다.
		
		this.account = account;
		
		account.setStatus(Account.Status.WORKING);// 단순하게, 할당된 시점부터를 working이라 잡는다.
	}
	
	public Account getAccount() {
		return account;
	}

	@Override
	public void run() {
		while(status != Status.DONE) {
			while(status == Status.WORKING) {// account, range 등이 exception 등에 의해 변경될 수 있다. 그 때 다시 working으로 돌리면서 진입한다.
				//TODO: filtering하다가 exception 난 것까지는 어떻게 할 수가 없다. 그것은 그냥 crawling 몇 번 한 평균으로서 그냥 보증한다.
				List<MediaFeedData> list;
				
				try {
					list = account.getListFromTag(tag, range.getMinimum(), range.getMaximum());
					
					/*
					 * 정상적일 때 - 일단 break 상태에서, 다른 task split한다.
					 * 다만 이 과정은 task를 다 가지고 있는 main에서 처리할 수 있기 때문에 callback해야 한다.
					 * 물론 0으로 resize부터 해준다.
					 */
					setRange(null);
					
					callback.onTaskSplit_();
					
				} catch (Exception e) {
					Result result = (Result) e;
					
					if(result.getStatus() == Result.Status.NORMAL) {
						// 일반적인 exception - 그냥 재시작하면 된다. 즉, break할 것도 없이, resize만 하면 된다. - split 유무 check 필요.
					} else if(result.getStatus() == Result.Status.LIMIT) {
						// limit exceeded - account change 하고 resize. 되면 그대로, 안되면 break. - split 유무 check 필요.
					} else {
						/*
						 * split 될 때 - 남은 부분을 split하고 resize.
						 * 특히 break된 task는 resize후 resume해준다.
						 * 다만 남은게 split limit 이하면 break된 task는 done으로 처리해서 빠져나가게 해준다.
						 */
					}
				}
			}
		}
	}

	public void splitTask(Task task, boolean interrupt) {
		Logger.getInstance().printMessage("<Task %d> Spliting", id);
		
		if(interrupt) {// 멈추게 되고, 결국 exception 내면서 callback할 것이다.
			account.interrupt(task);
			
			synchronized (this) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					Logger.getInstance().printException(e);
				}
			}
		} else {// 이미 pause된 task라면 list null 한 후 직접 callback하면 된다.
			callback.onTaskSplit(this, null, task);
		}
	}
	
	public void startTask() {
		Logger.getInstance().printMessage("<Task %d> Started", id);
		
		try {
			status = Status.WORKING;
			
			start();
		} catch(IllegalThreadStateException e) {
			Logger.getInstance().printException(e);
		}
	}
	
	public void stopTask() {
		Logger.getInstance().printMessage("<Task %d> Finished", id);
		
		status = Status.DONE;
	}
	
	public void pauseTask() {
		Logger.getInstance().printMessage("<Task %d> Paused", id);
		
		status = Status.UNAVAILABLE;
	}
	
	public void resumeTask() {
		Logger.getInstance().printMessage("<Task %d> Resumed", id);
		
		status = Status.WORKING;
	}
	
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public void setRange(Range<Long> range) {
		if(range != null) {
			Logger.getInstance().printMessage("<Task %d> Range arranged : %d, %d", id, range.getMinimum(), range.getMaximum());
		} else {
			Logger.getInstance().printMessage("<Task %d> Range arranged : null", id);
		}
		
		this.range = range;
	}
	
	public boolean setRange(long from, long to) {// to가 from 미만으로 갈 수도 있는 점을 boolean으로 정리한다.
		if(from <= to) {
			setRange(Range.between(from, to));
		} else {
			setRange(null);
		}
		
		return from <= to;
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
		
		void onTaskSplit_(Task task, );
	}
	
}
