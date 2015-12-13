package main.java.services.grep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Range;

import main.java.services.grep.Database.DatabaseCallback;
import main.java.services.grep.Task.TaskCallback;

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
		diff = upper - lower;
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
	
	public void printTaskProgress() {
		printTaskProgress(null, 0);
	}

	public synchronized void printTaskProgress(Task task, long visited) {
		this.visited += visited;
		
		Logger.printMessage("<Task %d> Progress : %d / %d. %.2f%% done in %s and %s remains.", task != null ? task.getTaskId() : -1, this.visited, diff, getTaskProgress(), getTaskElapsedTime(), getTaskRemainingTime());
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
	
	public boolean allocAccount(Task task) {// main 및 observer에서 access될 수 있으므로 sync.
		Account newAccount = null;
		
		for(Account account : accounts) {// 어차피 기존 account는 exception 날수도.
			account.updateStatus();
			
			if(account.getStatus() == Account.Status.FREE) {
				newAccount = account;
				
				break;
			}
		}
		
		if(newAccount != null) {
			task.setAccount(newAccount);
			
			Logger.printMessage("<Task %d - Account %d> Allocated", task.getTaskId(), newAccount.getAccountId());
			
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public void onTaskAccountDischarged(Task task, long bound) {
		synchronized (task) {
			Logger.printMessage("<Task %d> Need account", task.getTaskId());
			
			task.resizeTask(bound);// 지금 실패하고 observer에 의해 나중에 실행될 수도 있지만, 그럴 때 resize할 수가 없으므로, 여기서 미리 한다.
			
			if(allocAccount(task)) {// acc 할당 성공했을 때만 resume하고, 그렇지 않을 때는 그냥 observer로 돌면서 기다린다.
				task.resumeTask();;
			};
		}
	}

	@Override
	public void onTaskUnexpectedlyStopped(Task task, long bound) {
		synchronized (task) {
			Logger.printMessage("<Task %d> Stopped and restart", task.getTaskId());
			
			if(bound < task.getRange().getMaximum()) {// 그대로일 경우는 굳이 resize할 필요 없다.
				task.resizeTask(bound);
			}
			
			task.resumeTask();
		}
	}

	@Override
	public void onTaskJobCompleted(Task task) {
		synchronized (task) {
			Logger.printMessage("<Task %d> Job completed ", task.getTaskId());
			
			task.stopTask();
			
			task.resizeTask(task.getRange().getMinimum());// 0로 해두고 하면, logging이 깔끔하다.
		}
	}
	
	@Override
	public void onTaskIncompletelyFinished(Task task, long bound) {// 아무래도, 1개의 범위가 아닐 것이다.
		synchronized (task) {
			Logger.printMessage("<Task %d> Incompletely finished : %d, %d", task.getTaskId(), task.getRange().getMinimum(), bound);
			
			task.stopTask();
			
			task.resizeTask(bound);// stop 했어도 그래도 resizing 해두기는 한다.
		}
	}
	
	@Override
	public void onTaskResized(Task task, long size) {
		printTaskProgress(task, size);
	}

	/*
	 * 정확한 시간 추정을 위해서는 그냥 탐색하는 범위들의 sum을 diff에 대한 %로 계산하는 것이 제일 낫다.
	 * 물론 written도 별도로 표시할 수 있을 것이다.
	 */
	@Override
	public synchronized void onDatabaseWritten(int written) {
		done += written;
		
		Logger.printMessage("<Database> Progress : %d / %d. %.2f%% done.", done, size, getDatabaseProgress());
	}
	
	public float getDatabaseProgress() {
		return ((done / (float) size) * 100);
	}

	public void start() {
		Thread observer = new Observer();
		
		observer.start();
		
		try {
			observer.join();
		} catch (InterruptedException e) {
			Logger.printException(e);
		}
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
			
			Logger.printMessage("<Task %d> Range : %d to %d. diff is %d.", task.getTaskId(), min, max, max - min);
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
			Logger.printException(e);
		} catch(IOException e) {
			Logger.printException(e);
		} finally {
			try {
				reader.close();
			} catch(IOException e) {
				Logger.printException(e);
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
			
			while(!isAllTasksCompleted()) {
				for(Task task : tasks) {
					synchronized (task) {// task done 동시 체크까지 다 sync 잡을순 없어도 여기선 sync해줘야 한다.
						if(task.getStatus() == Task.Status.UNAVAILABLE) {
							if(task.getAccount() == null) {// 원래 초기화는 밖에서 하려 했으나, 이것도 마찬가지로 한번에 안될 수 있으므로 여기서 했다.
								if(allocAccount(task)) {
									task.startTask();
								}
							} else if(task.getAccount().getStatus() == Account.Status.UNAVAILABLE) {// exceeded
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
					Logger.printException(e);
				}
				
				printTaskProgress();// task resizing은 주기가 일정하지 않으므로, 이런 것도 필요하다고 생각한다.
			}
		}
		
	}

}
