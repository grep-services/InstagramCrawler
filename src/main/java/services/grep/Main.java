package main.java.services.grep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import main.java.services.grep.Task.TaskCallback;

import org.apache.commons.lang3.Range;

/**
 * 
 * 정확해야 하는데, 너무 size가 커졌다.
 * 신속하고 정확하게 만들기 위해, size를 줄인다.
 * instagram, 먹스타그램, multi-account
 * 필요한 exception들만 해결하는 방식
 * no-daemon, none-realtime
 * 
 * @author marine1079
 * @since 151025
 *
 */

public class Main implements TaskCallback {

	final String tag = "허니버터";// 24837/44274 정도.
	
	List<Account> accounts;
	List<Task> tasks;
	
	private long start;
	private long lower, upper, diff, visited;
	private long size, done;
	
	public Main() {
		initAccounts();// accounts는 file로 받는 것이 더 빠를듯.
		initSchedule();// task 하기 전에 schedule을 구성해야 한다.
		initTasks();// schedule에 맞게 tasks 구성해준다.
		
		start();
	}
	
	public void initSchedule() {
		lower = 0;
		upper = getLastItemId();
		diff = upper - lower + 1;
		visited = 0;
		size = getItemSize();
		done = 0;
		
		//diff = 1000000000000l;//10 ^ 12
		//lower = upper - diff;
	}
	
	// crawl해야 할 item의 total size를 구한다. 현재는 tag count로.
	public long getItemSize() {
		long size = -1;// default는 차라리 -1을 해야 logging에서 device by zero 피할 수 있다.
		
		for(Account account : accounts) {
			account.updateStatus();// 원래같으면 task에 할당되기 전에는 모두 unavailable이었겠지만, 뭐 그전에 free가 된다 해도 별 상관 없다.
			
			if(account.getStatus() == Account.Status.FREE) {
				size = account.getTagCount(tag);
				
				break;
			}
		}
		
		return size;
	}
	
	public long getLastItemId() {
		long id = 0;// 안나오면 그냥 0 to 0으로 끝난다.
		
		for(Account account : accounts) {
			account.updateStatus();// 원래같으면 task에 할당되기 전에는 모두 unavailable이었겠지만, 뭐 그전에 free가 된다 해도 별 상관 없다.
			
			if(account.getStatus() == Account.Status.FREE) {
				id = account.getLastMediaId(tag);
				
				break;
			}
		}
		
		return id;
	}
	
	public synchronized void showTaskProgress(Task task, long visited) {
		this.visited += visited;
		
		Logger.getInstance().printMessage("<Task %d> Travelled : %d / %d. %.2f%% done in %s and %s remains.", task != null ? task.getTaskId() : -1, this.visited, diff, getTaskProgress(), getTaskElapsedTime(), getTaskRemainingTime());
	}
	
	public float getTaskProgress() {
		return diff > 0 ? (visited / (float) diff) * 100 : 100;
	}
	
	public String getTaskElapsedTime() {
		long sec = (long) (System.currentTimeMillis() - start);
		
		Duration duration = Duration.ofMillis(sec);
		
		return duration.toString();
	}
	
	public String getTaskRemainingTime() {
		long elapsed = System.currentTimeMillis() - start;
		long remains = diff - visited;
		long sec = (long) (remains / ((float) visited) * elapsed);
		
		Duration duration = Duration.ofMillis(sec);
		
		return duration.toString();
	}
	
	public boolean isAllTasksCompleted() {
		boolean completed = true;
		
		for(Task task : tasks) {
			if(task.getStatus() != Task.Status.DONE) {
				completed = false;
				
				break;
			}
		}
		
		return completed;
	}
	
	/*
	 * 새 account 또는 자기 account를 check해서 alloc한다.
	 * 초기 alloc 뿐만 아니라 change를 위해서도 쓰인다.
	 * 특히 change일 때에는 미리 기존 account를 정리해줘야 한다.(free or unavailable)
	 * 사실상 이건, account와 task 개수가 같은 static 환경에서는 큰 의미가 없다.
	 * 차후 account 동적 추가 삭제 가능할 때나 필요할 기능이다.
	 */
	public boolean allocAccount(Task task) {
		for(Account account : accounts) {// 모든 account가 아니라, callback없는 account 또는 자기 자신의 account만 check한다.
			if(account.getCallback() == null || account.equals(task.getAccount())) {
				account.updateStatus();
				
				if(account.getStatus() == Account.Status.FREE) {
					if(account.getCallback() == null) {
						if(task.getAccount() != null) {// task가 이미 account 갖고 있으면 처리해준다.
							task.getAccount().setCallback(null);
							task.getAccount().updateStatus();
							
							task.setAccount(account);
						}
					} else {
						account.setStatus(Account.Status.WORKING);// 그냥 status만 바꿔주면 된다.
					}
					
					return true;
				}
			}
		}
		
		return false;
	}
	
	/*
	 * working이 하나라도 있다면 이 task는 interrupt를 하거나 아니면 그냥 pause로 계속 loop를 돌면서 observer의 처리를 기다린다.
	 * unavailable 중 account unavailable이 하나라도 있다면 range를 가져온다.(그러면 나머지 task들이 분배해갈 것이다.)
	 * 나머지 경우 즉 unavailable이면서 range null인 것들만 다 있다면 task는 done되면 된다.(다른 task들도 done될 것이다.)
	 */
	public boolean scheduleTask(Task task) {
		boolean done = true;
		boolean interrupted = false;
		
		for(Task task_ : tasks) {//TODO: 거의 다 되어가는데, DONE FALSE 부분 맞는지 정확히 CHECK. 왠지 SYNC 안으로 들어가야 하는 것일지도.
			if(task_.getStatus() == Task.Status.WORKING) {// loop에 들어간 것만 골라서(interruptable) split시킨다.
				/*
				 * working task가 있다는 자체로, interrupt 아니면 wait이니 아직 done될 때는 아니다.
				 * 만약 중간에 working이 unavailable등으로 된다 하더라도 어차피 observer에서 다시 해결될 것이며
				 * 사실상 monitor가 있기에 중간에 status change는 없을 것이다.
				 */
				done = false;
				
				synchronized (task_.scheduleMonitor) {// interruptable check시점부터가 정확히 필요한 시점이다.
					if(task_.isInterruptable()) {
						task_.interruptTask(task);
						
						interrupted = true;
						
						break;
					}
				}
			} else if(task_.getStatus() == Task.Status.UNAVAILABLE) {// 크게 discharged와 batch, free가 있다.
				if(task_.getAccount() == null) {// 시작시 null인 상태도 있다. 그냥 done false하고 pass한다.
					done = false;
				} else {
					if(task_.getAccount().getStatus() == Account.Status.UNAVAILABLE) {// discharged. range 교환한다.
						synchronized (task_.scheduleMonitor) {// 여기는 range null check시점부터가 정확한 시점이다.
							if(task_.getRange() != null) {
								done = false;// 아직 처리할 task가 남은 것이므로 done은 안된다.
								
								// range를 갖고온다.
								task.setRange(task_.getRange());
								task.resumeTask();
								
								task_.setRange(null);// alloc 시도까지 해줘도 문제없지만 이미 sync도 걸려있고 복잡하므로 observer에 맡긴다.
							}// discharged도 range null이면 일단 종료 조건은 된다.
						}
					} else {// working이다. range null(schedule 필요)인 경우와 not null(batch restart)인 경우가 있다.
						if(task_.getRange() != null) {// batch restart를 위한 것이며 done false해야한다.
							done = false;
						}// else인 scheduling 경우에는 done 유지된다.
					}
				}
			}
		}
		
		/*
		 * TODO:
		 * done을 추가하면 최종적으로 not schedulable로 done될 것 같지만, 100%로 안떨어지는 오차가 가끔 생긴다.
		 * 그 이유를 아직 잘 모르겠지만 일단 range null이 schedulable되어도 done만 되면 문제 없기 때문에 안전한 것으로 간다.
		 */
		if(!interrupted) {
			task.setSchedulable(true);// 이미 monitor 잡혀있다.
		}
		
		return done;
	}
	
	/*
	 * 당장 판단을 할 수 없는 경우도 있다. 그럴 때는 pasue한다.(working인데 interruptable은 아닌 경우들이 있을 때.)
	 */
	@Override
	public boolean onTaskScheduling(Task task) {// scheduling 성공여부를 return해서 .... 그런데 check는... range null, task status, interruptable 등이 있다. 적절히... 해보기.
		Logger.getInstance().printMessage("<Task %d> Free.", task.getTaskId());
		
		return scheduleTask(task);
	}

	@Override
	public boolean onTaskDischarged(Task task) {// account set 여부를 return해서 task의 status를 결정하게 해준다.
		Logger.getInstance().printMessage("<Task %d> Discharged.", task.getTaskId());
		
		return allocAccount(task);// 원래는 좀 condition 넣어서 다르게 가려다가, 단순화를 위해 그냥 이렇게 가기로 했다.
	}

	@Override
	public void onTaskTravelled(Task task, long visited) {
		showTaskProgress(task, visited);
	}

	@Override
	public void onTaskWritten(int written) {
		showDatabaseProgress(written);
	}
	
	public synchronized void showDatabaseProgress(int written) {
		done += written;
		
		Logger.getInstance().printMessage("<Database> Written : %d / %d. %.2f%% done.", done, size, getDatabaseProgress());
	}
	
	public float getDatabaseProgress() {
		return size > 0 ? ((done / (float) size) * 100) : 100;
	}

	public void start() {
		Thread observer = new Observer();
		
		observer.start();
		
		try {
			observer.join();
		} catch (InterruptedException e) {
			Logger.getInstance().printException(e);
		}
		
		Logger.getInstance().printMessage("<Task> All tasks finished.");
		
		Logger.getInstance().release();//TODO: 다른 release 대상 더있는지 찾아보기.
	}
	
	// 현재 알고리즘은 단순하다. schedule 만들고, schedule만큼 tasks 만든다.
	public void initTasks() {
		tasks = new ArrayList<Task>();
		
		// 간단하게 하려 해도 그 간단하게 하려는 것 때문에 복잡해진다. 그냥 풀어서 쓴다.
		if(diff == 0) {
			tasks.add(new Task(tag, Range.between(lower, lower), this));
		} else if(diff < accounts.size()) {// 상식적으로 accounts가 수백개가 될 리가 없다.
			tasks.add(new Task(tag, Range.between(lower, upper), this));
		} else {
			long size = diff / accounts.size();// 일단 교대 안해본다.(속도상 exceeded가 안생길 것 같기도 해서)
			
			for(int i = 0; i < accounts.size(); i++) {
				if(i < accounts.size() - 1) {
					tasks.add(new Task(tag, Range.between(lower + (i * size), lower + ((i + 1) * size - 1)), this));
				} else {
					tasks.add(new Task(tag, Range.between(lower + (i * size), upper), this));// n빵이 딱 떨어지는건 아니다.
				}
			}
		}
		
		for(Task task : tasks) {
			long min = task.getRange().getMinimum();
			long max = task.getRange().getMaximum();
			
			task.setTaskId(tasks.indexOf(task));
			
			Logger.getInstance().printMessage("<Task %d> Created : %d to %d. diff is %d.", task.getTaskId(), min, max, max - min);
		}
	}
	
	public void initAccounts() {
		accounts = new ArrayList<Account>();
		
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader("./src/accounts"));
			
			int index = 0;
			String line = null;
			while((line = reader.readLine()) != null) {
				if(line.startsWith("//")) {
					continue;
				}
				
				String[] array = line.split("\\s*,\\s*", 3);
				
				Account account = new Account(array[0], array[1], array[2]);
				
				account.setAccountId(index++);
				
				accounts.add(account);
			}
		} catch(FileNotFoundException e) {
			Logger.getInstance().printException(e);
		} catch(IOException e) {
			Logger.getInstance().printException(e);
		} finally {
			try {
				reader.close();
			} catch(IOException e) {
				Logger.getInstance().printException(e);
			}
		}
	}
	
	public static void main(String[] args) {
		new Main();
	}
	
	class Observer extends Thread {
		
		final long PERIOD = 1 * 60 * 1000 / 2;// 30초
		StringBuilder message;
		
		public Observer() {
			setDaemon(true);// exception, error 등에 대비하기 위함.
		}
		
		@Override
		public void run() {
			start = System.currentTimeMillis();
			
			while(true) {
				message = new StringBuilder("<Task> Status : ");
				
				for(Task task : tasks) {
					message.append(String.format("[T%d%s-%d, %s/%s]", task.getTaskId(), task.getStatus().getNick(), task.getRange() != null ? task.getRange().getMaximum() - task.getRange().getMinimum() : 0, task.getAccount() != null ? (task.isInterruptable() ? "I" : "NI") : "NI", task.isSchedulable() ? "S" : "NS"));
					
					if(task.getStatus() == Task.Status.UNAVAILABLE) {
						if(task.getAccount() == null) {// 원래 초기화는 밖에서 하려 했으나, 이것도 마찬가지로 한번에 안될 수 있으므로 여기서 했다.
							if(allocAccount(task)) {
								task.startTask();
							}
						} else {
							/*
							 * 아래의 if는 split을 기다리는 것이고, else 중의 account unavailable은 alloc을 기다리는 것이다.
							 * split을 기다리는 것 또한 alloc이 필요할 수는 있다.
							 * 다만 굳이 account update를 다시 하지 않아도, 어차피 rescheduled 이후의 account cycle에서 알아서 다시 limit exception 나든 될 것이다.
							 */
							if(task.getRange() == null) {
								synchronized (task.scheduleMonitor) {
									if(task.isSchedulable()) {
										task.setSchedulable(false);
										
										if(scheduleTask(task)) {
											task.stopTask();
										}
									}
								}
							} else {// 여기 들어왔다고 다 exceeded 아니다. batch limit 때문에 잠시 들어온 것들도 있는 만큼 filter해준다.
								if(task.getAccount().getStatus() == Account.Status.UNAVAILABLE) {// 어차피 loop랑 안겹쳐서 sync 필요없다.
									if(allocAccount(task)) {// 이미 pause, resize되어 있다. 할당해보고 되면 resume하고, 안되면 다시 pass.
										task.resumeTask();
									};
								}
							}
						}
					}// working task는 I/NI 둘 뿐인데 건들 것 없다.
				}
				
				Logger.getInstance().printMessage(message.toString());
				
				if(isAllTasksCompleted()) {
					break;
				}
				
				try {
					Thread.sleep(PERIOD);
				} catch(InterruptedException e) {
					Logger.getInstance().printException(e);
				}
			}
		}
		
	}

}
