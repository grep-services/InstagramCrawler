package main.java.services.grep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Range;

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

public class Main implements TaskCallback {

	final String tag = "조호찬";
	
	List<Account> accounts;
	List<Range<Long>> schedule;
	List<Task> tasks;
	
	public Main() {
		initAccounts();// accounts는 file로 받는 것이 더 빠를듯.
		initSchedule();// acconts 개수에 따라 schedule 단순 분할한다.(2/3정도로 해본다.)
		initTasks();// schedule에 맞게 tasks 구성해준다.
		
		start();
	}
	
	synchronized public void allocAccount(Task task) {// main 및 observer에서 access될 수 있으므로 sync.
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
			task.setStatus(Task.Status.WORKING);
			task.start();
		}
	}
	
	@Override
	public void onTaskAccountDischarged(Task task) {
		Printer.printException("Need account");
		
		task.setStatus(Task.Status.UNAVAILABLE);
		
		allocAccount(task);// 10분마다도 하지만, 필요할 때도 해준다.
	}

	@Override
	public void onTaskJobCompleted(Task task) {
		Printer.printException("Job Completed");
		
		task.setStatus(Task.Status.DONE);
	}

	public void start() {
		new Observer().start();
	}
	
	// 현재 알고리즘은 단순하다. schedule만큼만 tasks 만든다.
	public void initTasks() {
		tasks = new ArrayList<Task>();
		
		for(int i = 0; i < schedule.size(); i++) {
			Task task = new Task(tag, schedule.get(i), this);
			
			tasks.add(task);
		}
	}
	
	public void initSchedule() {
		long max = 1069993533294892048l;
		long size = max / accounts.size();
		
		schedule = new ArrayList<Range<Long>>();
		
		for(int i = 0; i < accounts.size(); i++) {
			if(i < accounts.size() - 1) {
				schedule.add(Range.between(i * size + 1, (i + 1) * size));
			} else {
				schedule.add(Range.between(i * size + 1, max));// n빵이 딱 떨어지는건 아니다.
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
		} catch (FileNotFoundException e) {
			Printer.printException(e.getMessage());
		} catch (IOException e) {
			Printer.printException(e.getMessage());
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				Printer.printException(e.getMessage());
			}
		}
	}
	
	public static void main(String[] args) {
		new Main();
	}
	
	class Observer extends Thread {

		final long PERIOD = 10 * 60 * 1000;// 10분
		
		@Override
		public void run() {
			while(true) {
				for(Task task : tasks) {
					if(task.getStatus() == Task.Status.UNAVAILABLE) {
						// 이렇게 하면 account가 할당되지 않은 첫 시작 및, exceeded된 나중까지 커버 가능하다.
						if(task.getAccount() == null || task.getAccount().getStatus() == Account.Status.UNAVAILABLE) {
							allocAccount(task);
						}
					}
				}
				
				try {
					Thread.sleep(PERIOD);
				} catch (InterruptedException e) {
					Printer.printException(e.getMessage());
				}
			}
		}
		
	}

}
