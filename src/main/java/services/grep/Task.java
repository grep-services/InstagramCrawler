package main.java.services.grep;

import java.util.List;

import main.java.services.grep.Database.DatabaseCallback;

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

public class Task extends Thread implements DatabaseCallback {

	public enum Status {// 단순히 boolean으로는 정확히 다룰 수가 없고, 그러면 꼬일 수가 있어서 이렇게 간다.
		UNAVAILABLE, WORKING, DONE;
	}
	public Status status;
	
	int id;
	
	Account account;
	Range<Long> range;
	String tag;
	
	private static final long SPLIT_LOWER_BOUND = 10000;// 대략 20 * LIMIT 정도로 해서 LIMIT 안넘을 정도로 잡았다.(중요한 기준은 아니다.)
	private static final long EXCEED_WAIT_TERM = 5 * 60 * 1000;// 5 minutes
	
	public Object statusMonitor = new Object();
	
	TaskCallback callback;
	
	public Task(Account account, Range<Long> range, int id, String tag, TaskCallback callback) {
		setDaemon(true);
		
		this.account = account;
		this.range = range;
		this.id = id;
		this.tag = tag;
		this.callback = callback;
		
		status = Status.UNAVAILABLE;
		
		Logger.getInstance().printMessage("<Task> Task %d created with range %d - %d (size : %d).", id, range.getMinimum(), range.getMaximum(), getSize());
	}
	
	public void startTask() {
		Logger.getInstance().printMessage("<Task> Task %d started.", id);
		
		try {
			status = Status.WORKING;
			
			start();
		} catch(IllegalThreadStateException e) {
			Logger.getInstance().printException(e);
		}
	}
	
	@Override
	public void run() {
		while(true) {
			synchronized (statusMonitor) {
				if(this.status == Status.DONE) {
					break;
				}
			}
			
			while(true) {
				synchronized (statusMonitor) {
					if(this.status != Status.WORKING) {
						break;
					}
				}
				
				Logger.getInstance().printMessage("<Task> Task %d is working.", id);
				
				//TODO: filtering하다가 exception 난 것(정보의 소실)까지는 어떻게 할 수가 없다. 그것은 그냥 crawling 몇 번 한 평균으로서 그냥 보증한다.
				Result result = account.getListFromTag(tag, range);
				
				Result.Status status = result.getStatus();
				List<MediaFeedData> list = result.getResult();
				
				writeListToDB(list);// 일단 db write부터.
				
				if(status == Result.Status.Empty) {// put to stack and pause.(callback)
					callback.onTaskTravelled(this, getSize());
					
					setRange(null);
					
					pauseTask();
					
					callback.onTaskEmpty(this);
				} else {
					long visited;
					
					if(list == null || list.isEmpty()) {
						visited = 0;
					} else {
						long id = extractId(list.get(list.size() - 1).getId());
						
						visited = range.getMaximum() - id + 1;// 결국 visited의 최대치는 max - min + 1 (range size)이다. => 보다 1 작다.
					}
					
					callback.onTaskTravelled(this, visited);
					
					setRange(range.getMinimum(), range.getMaximum() - visited);// visited가 최대치일 때는 결국 range는 1 차이로 null이 되어서 맞아떨어진다. => 1 작아서 null 불가능.
					
					if(status == Result.Status.Normal) {
						if(getSize() > SPLIT_LOWER_BOUND) {
							callback.onTaskSplitting(this);
						}
					} else if(status == Result.Status.Exceed) {
						try {
							Thread.sleep(EXCEED_WAIT_TERM);
						} catch (InterruptedException e) {
							Logger.getInstance().printException(e);
						}
					}
				}
			}
		}
	}
	
	public void writeListToDB(List<MediaFeedData> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		
		// 일단 1개로 진행해본다.
		Database database = new Database(list, this);
		
		database.start();
		
		/*
		 * 필요한 이유는, 최종적으로 보면 all task finished 이후 바로 종료되면
		 * db write를 하지 못한다.
		 * 그뿐만 아니라 task 자체가 done될 때에도 언제 task의 list ref가 죽을 지 모른다.
		 * 따라서 어차피 task는 thread이고 주기적 write도 아니므로 wait를 하도록 한다.
		 */
		try {
			database.join();
		} catch (InterruptedException e) {
			Logger.getInstance().printException(e);
		}
	}

	@Override
	public void onDatabaseWritten(int written) {
		callback.onTaskWritten(written);
	}
	
	public long extractId(String string) {
		return Long.valueOf(string.split("_")[0]);
	}
	
	public void pauseTask() {
		Logger.getInstance().printMessage("<Task> Task %d paused.", id);
		
		status = Status.UNAVAILABLE;
	}
	
	public void resumeTask() {
		Logger.getInstance().printMessage("<Task> Task %d resumed.", id);
		
		status = Status.WORKING;
	}
	
	public void stopTask() {
		Logger.getInstance().printMessage("<Task> Task %d stopped.", id);
		
		status = Status.DONE;
	}
	
	public void setRange(Range<Long> range) {
		this.range = range;
		
		if(range == null) {
			Logger.getInstance().printMessage("<Task> Task %d range set to 0.", id);
		} else {
			Logger.getInstance().printMessage("<Task> Task %d range set to %d - %d (size : %d).", id, range.getMinimum(), range.getMaximum(), getSize());
		}
	}
	
	public void setRange(long from, long to) {
		if(from <= to) {
			setRange(Range.between(from, to));
		} else {
			setRange(null);
		}
	}
	
	public Range<Long> getRange() {
		return range;
	}
	
	// 몇군데에서 쓰일 곳이 있다.
	public long getSize() {
		if(range == null) {
			return 0;
		} else {
			return range.getMaximum() - range.getMinimum() + 1;// at least 1
		}
	}
	
	public Status getStatus() {
		return status;
	}
	
	public int getTaskId() {
		return id;
	}
	
	public TaskCallback getCallback() {
		return callback;
	}

	public interface TaskCallback {
		void onTaskEmpty(Task task);
		void onTaskSplitting(Task task);
		void onTaskTravelled(Task task, long visited);
		void onTaskWritten(int written);
	}
	
}
