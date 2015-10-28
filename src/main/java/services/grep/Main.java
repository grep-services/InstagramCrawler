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

	final String tag = "먹스타그램";
	
	List<Account> accounts;
	List<Task> tasks;
	
	public Main() {
		initAccounts();// accounts는 file로 받는 것이 더 빠를듯.
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
	
	// 현재 알고리즘은 단순하다. schedule 만들고, schedule만큼 tasks 만든다.
	public void initTasks() {
		long min = 0;
		long max = 1069993533294892048l;
		long diff = max - min;
		
		tasks = new ArrayList<Task>();
		
		// 간단하게 하려 해도 그 간단하게 하려는 것 때문에 복잡해진다. 그냥 풀어서 쓴다.
		if(diff == 0) {
			tasks.add(new Task(tag, Range.between(min, min), this));
		} else if(diff < accounts.size()) {// 상식적으로 accounts가 수백개가 될 리가 없다.
			tasks.add(new Task(tag, Range.between(min, max), this));
		} else {
			long size = diff / accounts.size();// 일단 교대 안해본다.(속도상 exceeded가 안생길 것 같기도 해서)
			
			for(int i = 0; i < accounts.size(); i++) {
				if(i < accounts.size() - 1) {
					tasks.add(new Task(tag, Range.between(min + (i * size), min + ((i + 1) * size - 1)), this));
				} else {
					tasks.add(new Task(tag, Range.between(min + (i * size), max), this));// n빵이 딱 떨어지는건 아니다.
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
