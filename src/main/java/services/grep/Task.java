package main.java.services.grep;

import org.apache.commons.lang3.Range;

import main.java.services.grep.Account.AccountCallback;

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

public class Task extends Thread implements AccountCallback {

	public enum Status {// 단순히 boolean으로는 정확히 다룰 수가 없고, 그러면 꼬일 수가 있어서 이렇게 간다.
		UNAVAILABLE, WORKING, DONE;
	}
	public Status status;
	
	int id;
	String tag;
	Account account;
	Range<Long> range;
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
		if(this.account != null) {// 이미 쓰던게 있었다면
			this.account.updateStatus();// status 정리해준다.
		}
		
		this.account = account;
		
		account.setCallback(this);
		account.setStatus(Account.Status.WORKING);// 단순하게, 할당된 시점부터를 working이라 잡는다.
	}
	
	public Account getAccount() {
		return account;
	}

	@Override
	public void run() {
		Logger.printMessage("<Task %d> Running", getTaskId());
		
		while(status != Status.DONE) {
			while(status == Status.WORKING) {// account, range 등이 exception 등에 의해 변경될 수 있다. 그 때 다시 working으로 돌리면서 진입한다.
				Logger.printMessage("<Task %d> Running-in", getTaskId());
				
				account.getListFromTag(tag, String.valueOf(range.getMinimum()), String.valueOf(range.getMaximum()));
				
				synchronized (this) {//TODO: 유효성 CHECK는 해보기.
					try {
						wait();
					} catch (InterruptedException e) {
						Logger.printException(e);
					}
				}
			}
		}
	}
	
	@Override
	public void onAccountLimitExceeded(Long bound) {// limit exceeded - 새 account를 요청해야 한다.
		Logger.printMessage("<Account> Limit exceeded");

		account.setStatus(Account.Status.UNAVAILABLE);// acc 변화가 빨라야 observer와의 충돌이 안생긴다.
		
		callback.onTaskAccountDischarged(this, bound);// ACC : UNAVAILABLE, TASK : UNAVAILABLE
	}

	@Override
	public void onAccountExceptionOccurred(Long bound) {// account occur - 다시 실행되어야 한다.
		Logger.printMessage("<Account> Exception occurred");

		callback.onTaskUnexpectedlyStopped(this, bound);// ACC : WORKING, TASK : UNAVAILABLE
	}

	@Override
	public void onAccountRangeDone() {// range done - 끝난 것.
		Logger.printMessage("<Account %d> Range done", getTaskId());
		
		account.updateStatus();// 다 썼으니까 refresh 한번 해준다.(working 상태가 아니게 만드는 의미도 있다.)
		
		callback.onTaskJobCompleted(this);// ACC : 모르고, TASK : DONE. BREAK;
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
		void onTaskAccountDischarged(Task task, long bound);
		void onTaskUnexpectedlyStopped(Task task, long bound);
		void onTaskJobCompleted(Task task);
		void onTaskIncompletelyFinished(Task task, long bound);
	}
	
}
