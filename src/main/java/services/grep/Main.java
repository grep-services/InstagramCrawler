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

	final String tag = "먹스타그램";
	
	List<Account> accounts;
	List<Task> tasks;
	
	private long start, finish;
	private long lower = 0, upper = 1069993533294892048l, diff = upper - lower;
	private long done = 0;
	
	public Main() {
		initAccounts();// accounts는 file로 받는 것이 더 빠를듯.
		initTasks();// schedule에 맞게 tasks 구성해준다.
		
		start();
	}
	
	public void stopTask(Task task) {
		task.setStatus(Task.Status.DONE);
	}
	
	public void pauseTask(Task task) {
		task.setStatus(Task.Status.UNAVAILABLE);
	}
	
	public void resumeTask(Task task) {// 어차피 alloc 안에서 run되므로 sync 또 할 필요 없다.
		task.setStatus(Task.Status.WORKING);
	}
	
	public void startTask(Task task) {
		try {
			task.setStatus(Task.Status.WORKING);
			
			task.start();
		} catch(IllegalThreadStateException e) {
			Logger.printException(e);
		}
	}
	
	public void resizeTask(Task task, long bound) {
		task.setRange(Range.between(task.getRange().getMinimum(), bound));
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
	
	public synchronized boolean allocAccount(Task task) {// main 및 observer에서 access될 수 있으므로 sync.
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
			
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public void onTaskAccountDischarged(Task task, long bound) {
		Logger.printMessage("<Task> Need account");
		
		pauseTask(task);
		
		resizeTask(task, bound);// 지금 실패하고 observer에 의해 나중에 실행될 수도 있지만, 그럴 때 resize할 수가 없으므로, 여기서 미리 한다.
		
		if(allocAccount(task)) {// acc 할당 성공했을 때만 resume하고, 그렇지 않을 때는 그냥 observer로 돌면서 기다린다.
			resumeTask(task);
		};
	}

	@Override
	public void onTaskUnexpectedlyStopped(Task task, long bound) {
		Logger.printMessage("<Task> Stopped and restart");
		
		pauseTask(task);
		
		resizeTask(task, bound);
		
		resumeTask(task);
	}

	@Override
	public void onTaskJobCompleted(Task task) {
		Logger.printMessage("<Task> Job completed");
		
		stopTask(task);
	}
	
	@Override
	public void onTaskIncompletelyFinished(Task task, long bound) {// 아무래도, 1개의 범위가 아닐 것이다.
		Logger.printMessage("<Task> Incompletely finished : " + task.getRange().getMinimum() + ", " + bound);
		
		stopTask(task);
		
		resizeTask(task, bound);// stop 했어도 그래도 resizing 해두기는 한다.
	}

	@Override
	public synchronized void onDatabaseWritten(int written) {
		done += written;
		
		Logger.printMessage(String.format("<Database> Progress : %s / %s. %.2f\\% done. %s remains.", done, diff, getProgress(), getRemainingTime()));
	}
	
	public float getProgress() {
		return ((done / (float)diff) * 100);
	}
	
	public String getRemainingTime() {
		long sec = (long)((finish - start) * (diff / (float)done));
		
		Duration duration = Duration.ofSeconds(sec);
		
		return duration.toString();
	}

	public void start() {
		new Observer().start();
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
	}
	
	public void initAccounts() {
		accounts = new ArrayList<Account>();
		
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader("./src/accounts"));
			
			String line = null;
			while((line = reader.readLine()) != null) {
				String[] array = line.split("\\s*,\\s*", 3);
				
				accounts.add(new Account(array[0], array[1], array[2]));
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
					if(task.getStatus() == Task.Status.UNAVAILABLE) {
						if(task.getAccount() == null) {// 원래 초기화는 밖에서 하려 했으나, 이것도 마찬가지로 한번에 안될 수 있으므로 여기서 했다.
							if(allocAccount(task)) {
								startTask(task);
							}
						} else if(task.getAccount().getStatus() == Account.Status.UNAVAILABLE) {// exceeded
							if(allocAccount(task)) {// 이미 pause, resize되어 있다. 할당해보고 되면 resume하고, 안되면 다시 pass.
								resumeTask(task);
							};
						}// working일 경우는, 사실 exception이 났을 경운데, 그 경우는 처리되었을 것이라고 본다.
					}
				}
				
				try {
					Thread.sleep(PERIOD);
				} catch(InterruptedException e) {
					Logger.printException(e);
				}
			}
			
			finish = System.currentTimeMillis();
		}
		
	}

}
