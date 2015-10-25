package java.services.grep;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class Main {

	final String tag = "먹스타그램";
	
	List<Account> accounts;
	List<Range<Long>> schedule;
	List<Task> tasks;
	
	public Main() {
		initAccounts();// accounts는 file로 받는 것이 더 빠를듯.
		initSchedule();// acconts 개수에 따라 schedule 단순 분할한다.
		initTasks();// 아무리 accounts가 정해져있어도, 그래도 이정도는 받아서 tasks 바로 구성해준다.
		
		start();
	}
	
	public void start() {
		for(Task task : tasks) {
			task.start();
		}
	}
	
	// 현재 알고리즘은 단순하다. accounts 절반으로 tasks 만들고, 나머지는 보충용도로 쓴다.
	public void initTasks() {
		tasks = new ArrayList<Task>();
		
		for(int i = 0; i < accounts.size(); i++) {
			Task task = new Task(tag, accounts.get(i), schedule.get(i));
			
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
			reader = new BufferedReader(new FileReader("accounts"));
			
			String line = null;
			while((line = reader.readLine()) != null) {
				String[] array = line.split("\\s*,\\s*", 3);
				
				accounts.add(new Account(array[0], array[1], array[2]));
			}
		} catch (FileNotFoundException e) {
			Logger.printException(e.getMessage());
		} catch (IOException e) {
			Logger.printException(e.getMessage());
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				Logger.printException(e.getMessage());
			}
		}
	}
	
	public static void main(String[] args) {
		new Main();
	}

}
