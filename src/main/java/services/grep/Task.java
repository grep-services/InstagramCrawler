package main.java.services.grep;

import java.util.List;

import main.java.services.grep.Account.AccountCallback;
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

public class Task extends Thread implements AccountCallback, DatabaseCallback {

	public enum Status {// 단순히 boolean으로는 정확히 다룰 수가 없고, 그러면 꼬일 수가 있어서 이렇게 간다.
		UNAVAILABLE("U"), WORKING("W"), DONE("D");
		
		private String nick;
		
		Status(String nick) {
			this.nick = nick;
		}
		
		public String getNick() {
			return nick;
		}
	}
	public Status status;
	
	int id;
	String tag;
	Account account;
	Range<Long> range;
	
	private static final long BOUND = 10000;// 대략 20 * LIMIT 정도로 해서 LIMIT 안넘을 정도로 잡았다.(중요한 기준은 아니다.)
	
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
		this.account = account;
		
		account.setStatus(Account.Status.WORKING);// 단순하게, 할당된 시점부터를 working이라 잡는다.
		account.setCallback(this);
	}
	
	public Account getAccount() {
		return account;
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

	@Override
	public void run() {
		while(status != Status.DONE) {
			while(status == Status.WORKING) {// account, range 등이 exception 등에 의해 변경될 수 있다. 그 때 다시 working으로 돌리면서 진입한다.
				//TODO: filtering하다가 exception 난 것(정보의 소실)까지는 어떻게 할 수가 없다. 그것은 그냥 crawling 몇 번 한 평균으로서 그냥 보증한다.
				List<MediaFeedData> list;
				
				try {// 정상적일 때 - 일단 break 상태에서, 다른 task split한다.
					list = account.getListFromTag(tag, range.getMinimum(), range.getMaximum());
					
					writeListToDB(list);// 일단 db write부터.
					
					callback.onTaskTravelled(this, range.getMaximum() - range.getMinimum() + 1);
					
					setRange(null);// 0으로 resize도 해준다.(사실 상징적인 의미)
					
					pauseTask();// 미리 break되도록 해놓으면 아래 method에서 status가 바로 바뀌든 나중에 바뀌든 문제없이 돌아갈 것이다.
					
					if(callback.onTaskFree(this)) {// task를 다 가지고 있는 main에서 처리할 수 있기 때문에 callback해야 한다.
						stopTask();
					}// else의 경우는 그냥 callback의 실행 내용에 따르며 range, status 등도 알아서 결정될 것이다.
				} catch (Exception e) {
					Result result = (Result) e;
					
					list = result.getResult();
					
					writeListToDB(list);// 일단 db write부터.
					
					// 받은 만큼만 세어주는데, null이면 당연히 0개이다.(다시 그대로 재시작)
					callback.onTaskTravelled(this, (list != null && !list.isEmpty()) ? range.getMaximum() - extractId(list.get(list.size() - 1).getId()) + 1 : 0);
					
					setRange(range.getMinimum(), (list != null && !list.isEmpty()) ? extractId(list.get(list.size() - 1).getId()) - 1 : range.getMaximum());
					
					/*
					 * range가 null이라면 exception이 무엇이든 무시하고 그냥 위의 것처럼 처리한다.
					 * 여기서도 observer에서처럼 limit와 겹칠수도 있지만
					 * 마찬가지로 scheduling되고 나서 어차피 limit가 여전히 문제라면 거기서 exception에 걸려서 처리될 것이므로 문제없다.
					 */
					if(range == null) { 
						pauseTask();// 이것도 역시 미리 break해둔다.
						
						if(callback.onTaskFree(this)) {
							stopTask();
						}
					} else {// 일반적인 exception 처리. element가 단 1개라도 있다는 이야기도 된다.
						Task task = result.getTask();// 필요하다면 this를 split해서 task에게 나눠줄 것이다.
						
						if(result.getStatus() == Result.Status.NORMAL) {// 일반적인 exception - split 있을 때만 하면 된다.(없을 때는 위에서 이미 range set)
							if(task != null) {
								splitTask(this, task);
							}
						} else if(result.getStatus() == Result.Status.LIMIT) {// limit exceeded - account change. 안되면 break. split 유무 check 필요.
							if(!callback.onTaskDischarged(this)) {// split에 상관없이 change가 실패하면 break 예약해둔다.
								pauseTask();
							}// else는 그냥 놔두면 된다.
							
							if(task != null) {
								splitTask(this, task);
							}
						} else {// split - 그냥 하면 된다.
							splitTask(this, task);
						}
					}
				}
			}
		}
	}
	
	public long extractId(String string) {
		return Long.valueOf(string.split("_")[0]);
	}
	
	// this를 interrupt해서 task_도 나눠 받아라는 method.
	public void interruptTask(Task task_) {
		account.interrupt(task_);// account break한 다음에 exception을 통해 this의 while로 넘어오려는 계획.
	}
	
	public boolean isInterruptable() {
		return account.getInterruptable();
	}
	
	/*
	 * limit, normal, split 등 어디에서도 interrupted되어서 온 것에게 통일적으로 완벽한 split을 해주기 위한 내부 method.
	 * task range not null이므로 task를 쪼개서 task_에게 준다.
	 * 쪼개서 나눴을 때는, task는 물론 working status에서 온 것이지만 task_는 unavailable status일 수가 있는 만큼 resume을 시켜준다.
	 * task는 monitor 계속 유지되는 것은 아니지만 그 안에서 interruptable이 처리되므로 결국 lock 적용된다고 보아서 sync 필요없다고 생각했고,
	 * task_는 range null callback되는 것도 결국 단일 call이 되기 때문에 interruptable에 문제가 안생기고 sync 필요없다고 판단했다. 
	 */
	private void splitTask(Task task, Task task_) {// 일단 sync할 필요 없을 것 같아서 안했다.
		long size = task.getRange().getMaximum() - task.getRange().getMinimum();
		
		if(size > BOUND) {
			Logger.getInstance().printMessage("<Task %d> Split and share with Task %d.", id, task_.getTaskId());
			
			long pivot = size / 2;
			
			task.setRange(task.getRange().getMinimum(), task.getRange().getMinimum() + pivot);
			
			task_.setRange(task.getRange().getMinimum() + pivot + 1, task.getRange().getMaximum());// size >= 1 만 되어도 이 range는 최소 size 1이 되어서 문제없다.
			task_.resumeTask();
		}// bound보다 작은 것에 대해서는, task가 그대로 떠맡을 것이고, task_는 그대로 unavailable을 유지할 것이다.
	}
	
	public void startTask() {
		Logger.getInstance().printMessage("<Task %d> Started.", id);
		
		try {
			status = Status.WORKING;
			
			start();
		} catch(IllegalThreadStateException e) {
			Logger.getInstance().printException(e);
		}
	}
	
	public void stopTask() {
		Logger.getInstance().printMessage("<Task %d> Done.", id);
		
		status = Status.DONE;
	}
	
	public void pauseTask() {
		Logger.getInstance().printMessage("<Task %d> Paused.", id);
		
		status = Status.UNAVAILABLE;
	}
	
	public void resumeTask() {
		Logger.getInstance().printMessage("<Task %d> Resumed.", id);
		
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
	
	/*
	 * to가 from 미만으로 갈 수도 있는 점을 boolean으로 정리한다.
	 * split 등등 여기서 모든 travel callback을 하지 못하는 이유가 있다. 그냥 꼼꼼히 직접 다 해준다.
	 */
	public boolean setRange(long from, long to) {
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
		boolean onTaskFree(Task task);
		boolean onTaskDischarged(Task task);
		void onTaskTravelled(Task task, long visited);
		void onTaskWritten(int written);
	}
	
}
