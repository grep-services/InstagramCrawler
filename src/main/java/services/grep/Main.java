package main.java.services.grep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import main.java.services.grep.Database.DatabaseCallback;
import main.java.services.grep.Task.TaskCallback;

import org.apache.commons.lang3.Range;
import org.jinstagram.entity.users.feed.MediaFeedData;

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

public class Main implements TaskCallback, DatabaseCallback {

	final String tag = "ㅂㅈ";
	
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
	
	public void processTaskProgress(Task task, long from, long to) {
		processTaskProgress(task, to - from + 1);
	}
	
	public synchronized void processTaskProgress(Task task, long visited) {
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
			synchronized (task) {// 이왕이면 하는게 낫다.
				if(task.getStatus() != Task.Status.DONE) {
					completed = false;
					
					break;
				}
			}
		}
		
		return completed;
	}
	
	/*
	 * 무조건 새로 alloc이 아니라, 자기것부터 검사하고
	 * 자기것을 못쓰면 alloc하는 방식으로 간다.
	 * 물론 자기가 null을 가질 때는 그냥 alloc하는 것도 포함이다.
	 */
	public boolean allocAccount(Task task) {
		synchronized (task) {
			if(task.getAccount() != null && task.getAccount().updateStatus() == Account.Status.WORKING) {
				return true;
			}
			
			for(Account account : accounts) {
				synchronized (account) {// 한 acc가 여러 task에 가지 않도록 lock.
					account.updateStatus();
					
					if(account.getStatus() == Account.Status.FREE) {
						task.setAccount(account);
						
						return true;
					}
				}
			}
			
			return false;
		}
	}
	
	public boolean allocSchedule(Task task) {
		for(Task task_ : tasks) {
			synchronized (task_) {
				if(!task_.equals(task) && task_.getStatus() != Task.Status.DONE) {// done만 아니면 된다.
					if(task_.getStatus() == Task.Status.WORKING) {
						task_.splitTask(task, true);
					} else {
						task_.splitTask(task, false);
					}
					
					return true;
				}
			}
		}
		
		return false;
	}
	
	@Override
	public void onTaskDone(Task task, List<MediaFeedData> list) {// 남의걸 갖고와서 자기가 나눠가진다.
		writeListToDB(list);
		
		synchronized (task) {
			processTaskProgress(task, task.getRange().getMinimum(), task.getRange().getMaximum());
			
			task.setRange(null);
			
			if(!allocSchedule(task)) {// 아무것도 할당 못했다 - 다 끝났다.
				task.stopTask();
			}// alloc schedule이 제대로 되었다면 나머지는 onsplit에서 다 처리될 것이다.
		}
	}

	@Override
	public void onTaskStop(Task task, List<MediaFeedData> list) {// 자기것을 다시 실행한다.
		writeListToDB(list);
		
		synchronized (task) {
			if(list != null && !list.isEmpty()) {
				long last = extractId(list.get(list.size() - 1).getId());
				
				processTaskProgress(task, last, task.getRange().getMaximum());
				
				if(task.setRange(task.getRange().getMinimum(), last - 1)) {
					if(!allocAccount(task)) {// account가 없다. - 그냥 기다린다.
						task.pauseTask();
					} else {
						task.resumeTask();
					}
				} else {// stop도 done과 같은 stop이 있을 수 있다.
					task.setRange(task.getRange().getMinimum(), task.getRange().getMaximum());
					
					if(!allocSchedule(task)) {// 아무것도 할당 못했다 - 다 끝났다.
						task.stopTask();
					} else {
						if(!allocAccount(task)) {// account가 없다. - 그냥 기다린다.
							task.pauseTask();
						} else {
							task.resumeTask();
						}
					}
				}
			} else {
				processTaskProgress(task, 0);
				
				if(!allocAccount(task)) {// account가 없다. - 그냥 기다린다.
					task.pauseTask();
				} else {
					task.resumeTask();
				}
			}
		}
	}
	
	/*
	 * 어느정도 구현은 되었는데
	 * 아직 done 조건이 정확하지 않고,
	 * negative integer 가능성 명확히 확인해야 하며
	 * sync도 확인해야 되고
	 * 전체 process 검증 및
	 * 다시한번 code 최적화 한다.
	 */
	
	@Override
	public void onTaskSplit(Task task, List<MediaFeedData> list, Task task_) {// 절반만 다시 실행한다.
		writeListToDB(list);
		
		long last, min, pivot;
		
		synchronized (task) {
			if(list != null && !list.isEmpty()) {
				last = extractId(list.get(list.size() - 1).getId());
			} else {
				last = task.getRange().getMaximum() + 1;
			}
			
			processTaskProgress(task, last, task.getRange().getMaximum());
			
			min = task.getRange().getMinimum();
			pivot = (min + (last - 1)) / 2;
			
			// 거의 마지막이 되면 둘 중 한개는 먼저 stop(done)이 될 것이다.
			if(task.setRange(min, pivot)) {
				if(!allocAccount(task)) {// account가 없다. - 그냥 기다린다.
					task.pauseTask();
				} else {
					task.resumeTask();
				}
			} else {
				task.stopTask();
			}
		}
		
		synchronized (task_) {
			if(task_.setRange(pivot + 1, last - 1)) {
				if(!allocAccount(task_)) {// account가 없다. - 그냥 기다린다.
					task_.pauseTask();
				} else {
					task_.resumeTask();
				}
			} else {
				task_.stopTask();
			}
		}
	}

	public long extractId(String string) {
		return Long.valueOf(string.split("_")[0]);
	}
	
	public void writeListToDB(List<MediaFeedData> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		
		// 일단 1개로 진행해본다.
		Database database = new Database(list, this);
		
		database.start();
		
		/*
		 * 주기적 write가 아닌만큼 wait해도 무방하다 생각
		 * 문제 있다면 없앤다.
		 */
		try {
			database.join();
		} catch (InterruptedException e) {
			Logger.getInstance().printException(e);
		}
	}

	/*
	 * 정확한 시간 추정을 위해서는 그냥 탐색하는 범위들의 sum을 diff에 대한 %로 계산하는 것이 제일 낫다.
	 * 물론 written도 별도로 표시할 수 있을 것이다.
	 */
	@Override
	public synchronized void onDatabaseWritten(int written) {
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
			observer.join();//TODO: 나중에는 task들마다 join 달아서 observer와 상관없이 바로 끝낼 수 있도록(물론 observer도 종료되게) 해본다.
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
		
		final long PERIOD = 5 * 60 * 1000;// 5분
		
		public Observer() {
			setDaemon(true);
		}
		
		@Override
		public void run() {
			start = System.currentTimeMillis();
			
			while(true) {
				for(Task task : tasks) {
					synchronized (task) {// task done 동시 체크까지 다 sync 잡을순 없어도 여기선 sync해줘야 한다.
						if(task.getStatus() == Task.Status.UNAVAILABLE) {
							if(task.getAccount() == null) {// 원래 초기화는 밖에서 하려 했으나, 이것도 마찬가지로 한번에 안될 수 있으므로 여기서 했다.
								if(allocAccount(task)) {
									task.startTask();
								}
							} else {
								if(allocAccount(task)) {// 이미 pause, resize되어 있다. 할당해보고 되면 resume하고, 안되면 다시 pass.
									task.resumeTask();
								};
							}// working일 경우는, 사실 exception이 났을 경운데, 그 경우는 처리되었을 것이라고 본다.
						}
					}
				}
				
				try {
					Thread.sleep(PERIOD);
				} catch(InterruptedException e) {
					Logger.getInstance().printException(e);
				}
				
				if(isAllTasksCompleted()) {
					break;
				}
			}
		}
		
	}

}
