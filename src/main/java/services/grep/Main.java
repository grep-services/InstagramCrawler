package main.java.services.grep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import main.java.services.grep.Task.TaskCallback;

import org.apache.commons.lang3.Range;

/**
 * 
 * 정확해야 하는데, 너무 size가 커졌다.
 * 신속하고 정확하게 만들기 위해, size를 줄인다.
 * instagram, 먹스타그램, multi-account
 * 필요한 exception들만 해결하는 방식
 * no-daemon, none-realtime, + not-async.
 * 
 * @author marine1079
 * @since 151025
 *
 */

public class Main implements TaskCallback {

	String tag;
	
	List<Account> accounts;
	List<Task> tasks;
	
	private long start;
	private long lower, upper, diff, visited;
	private long size, done;
	
	private static final int DIFF_LOWER_BOUND = 1000;// account 조건 단순화를 위한 의미가 크다. 이걸 안쓰려면 account size로 해도 되긴 할 것이다.
	
	private Object taskMonitor = new Object();
	private Object databaseMonitor = new Object();
	
	private Stack<Task> stack = new Stack<Task>();
	
	public Main() {
		initAccounts();
		
		if(accounts == null || accounts.isEmpty()) {// 작지만 모든건 확실히. task 조건 등에서 분명히 accounts null check 필요성들 있다.
			Logger.getInstance().printMessage("<Main> At least 1 account is needed.");
		} else {
			Logger.getInstance().printMessage("<Main> %d accounts initiated.", accounts.size());
			
			if(initCondition()) {
				start = System.currentTimeMillis();// 더이상 실패 조건이 없다.
				
				initTasks();
				
				startAllTasks();
				
				waitAllTasks();
			}
		}
	}
	
	public void initAccounts() {
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
				
				if(accounts == null) {
					accounts = new ArrayList<Account>();
				}
				
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
	
	public boolean initCondition() {
		tag = "먹스타그램";// 24837/44274 정도.
		
		lower = 0;
		
		upper = getLastItemId();
		if(upper < 0) {
			Logger.getInstance().printMessage("<Main> Upper bound must be equal or bigger than 0.");
			
			return false;
		} else {
			Logger.getInstance().printMessage("<Main> Upper bound : %d", upper);
		}
		
		diff = upper - lower + 1;
		if(diff < DIFF_LOWER_BOUND) {
			Logger.getInstance().printMessage("<Main> Diff must be equal or bigger than %d", DIFF_LOWER_BOUND);
			
			return false;
		} else {
			Logger.getInstance().printMessage("<Main> Diff : %d", diff);
		}
		
		visited = 0;
		
		size = getItemSize();
		if(size < 0) {
			Logger.getInstance().printMessage("<Main> Size must be equal or bigger than 0.");
			
			return false;
		} else {
			Logger.getInstance().printMessage("<Main> Size : %d", size);
		}
		
		done = 0;
		
		return true;
	}
	
	// crawl해야 할 item의 total size를 구한다. 현재는 tag count로.
	public long getItemSize() {
		long size = -1;// default는 차라리 -1을 해야 logging에서 device by zero 피할 수 있다.
		
		for(Account account : accounts) {
			if(account.getRateRemaining() > 0) {
				size = account.getTagCount(tag);
				
				break;
			}
		}
		
		return size;
	}
	
	public long getLastItemId() {
		long id = -1;
		
		for(Account account : accounts) {
			if(account.getRateRemaining() > 0) {
				id = account.getLastMediaId(tag);
				
				break;
			}
		}
		
		return id;
	}
	
	/*
	 * account, range, id, tag, callback을 가지는 task를 만든다.
	 * 모두 set method를 만들어서 사용할 수도 있었지만,
	 * 저 요소들은 task를 구성하는 필요조건이므로 constructor에 넣도록 했다.
	 */
	public void initTasks() {
		long unit = diff / accounts.size();// 일단 교대 안해본다.(속도상 exceeded가 안생길 것 같기도 해서)
		
		tasks = new ArrayList<Task>();// accounts assure the empty-ness of the tasks
		
		for(int i = 0; i < accounts.size(); i++) {
			Range<Long> range;
			
			if(i < accounts.size() - 1) {
				range = Range.between(lower + (i * unit), lower + ((i + 1) * unit - 1));
			} else {
				range = Range.between(lower + (i * unit), upper);// n빵이 딱 떨어지는건 아니다.
			}
			
			Task task = new Task(accounts.get(i), range, i, tag, this);
			
			tasks.add(task);
		}
	}
	
	// list에 추가하면서 start시키면 바로 끝나면서 condition check하는 task들 때문에 문제가 생긴다.
	public void startAllTasks() {
		for(Task task : tasks) {
			task.startTask();
		}
	}
	
	public void waitAllTasks() {
		for(Task task : tasks) {
			try {
				task.join();
			} catch (InterruptedException e) {
				Logger.getInstance().printException(e);
			}
		}
		
		Logger.getInstance().printMessage("<Status> All tasks stopped.");
	}
	
	@Override
	public void onTaskEmpty(Task task) {// stack 자체도 thread safe하고, 각 line별로 sync 필요성은 없다.
		Logger.getInstance().printMessage("<Task> Task %d range got empty.", task.getTaskId());
		
		stack.push(task);
		
		for(Task task_ : tasks) {
			System.out.print(String.format("[%d:%d]", task_.getTaskId(), task_.getSize()));
		}
		
		System.out.print(String.format(" - %d, %d\n", stack.size(), tasks.size()));
		
		if(stack.size() == tasks.size()) {// exit condition
			Logger.getInstance().printMessage("<Main> Trying to stop all tasks...");
			
			stopAllTasks();
		}
	}

	@Override
	public void onTaskSplitting(Task task) {
		if(!stack.isEmpty()) {
			Task task_ = stack.pop();// thread safe
			
			long min = task.getRange().getMinimum();
			long max = task.getRange().getMaximum();
			long pivot = (max - min) / 2;// size로 구해버리면 p는 [a, b]가 되어 힘들다. 이렇게 해야 [a, b)가 된다.
			
			task_.setRange(Range.between(min, min + pivot));
			synchronized (task_.statusMonitor) {
				task_.resumeTask();//TODO: LOCK 걸고 해야될지도.
			}
			
			task.setRange(Range.between(min + pivot + 1, max));// 이왕이면 하던거 이어서 하게 해준다.
			
			Logger.getInstance().printMessage("<Task> Task %d splitted via task %d.", task.getTaskId(), task_.getTaskId());
		} else {
			Logger.getInstance().printMessage("<Task> Task %d will go on - the stack is empty.", task.getTaskId());
		}
	}
	
	/*
	 * make all tasks' status done.
	 * print log.
	 */
	public void stopAllTasks() {
		for(Task task : tasks) {
			task.stopTask();
		}
		
		Logger.getInstance().release();
	}
	
	@Override
	public void onTaskTravelled(Task task, long visited) {
		showTaskProgress(task, visited);
	}
	
	public void showTaskProgress(Task task, long visited) {
		synchronized (taskMonitor) {
			this.visited += visited;
			
			Logger.getInstance().printMessage("<Main> Task %d travelled %d, totally %d of %d (%.2f%%), elapsed %s, remains %s.", task.getTaskId(), visited, this.visited, diff, getTaskProgress(), getTaskElapsedTime(), getTaskRemainingTime());
		}
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

	@Override
	public void onTaskWritten(int written) {
		showDatabaseProgress(written);
	}
	
	public void showDatabaseProgress(int written) {
		synchronized (databaseMonitor) {
			done += written;
			
			Logger.getInstance().printMessage("<Main> Data written %d of %d (%.2f%%).", done, size, getDatabaseProgress());
		}
	}
	
	public float getDatabaseProgress() {
		return size > 0 ? ((done / (float) size) * 100) : 100;
	}
	
	public static void main(String[] args) {
		new Main();
	}

}
