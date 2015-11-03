package main.java.services.grep;

import java.nio.file.AccessMode;
import java.util.List;

import org.apache.commons.lang3.Range;
import org.jinstagram.entity.users.feed.MediaFeedData;

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
	
	String tag;
	Account account;
	Range<Long> range;
	TaskCallback callback;
	
	public Task(String tag, Range<Long> range, TaskCallback callback) {
		this.tag = tag;
		this.range = range;
		this.callback = callback;
		
		status = Status.UNAVAILABLE;
		
		setDaemon(true);
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
		while(status != Status.DONE) {
			while(status == Status.WORKING) {// account, range 등이 exception 등에 의해 변경될 수 있다. 그 때 다시 working으로 돌리면서 진입한다.
				account.getListFromTag(tag, String.valueOf(range.getMinimum()), String.valueOf(range.getMaximum()));
			}
		}
	}
	
	@Override
	public void onAccountLimitExceeded(Long bound) {// limit exceeded - 새 account를 요청해야 한다.
		Logger.printException("Limit exceeded");

		account.setStatus(Account.Status.UNAVAILABLE);// acc 변화가 빨라야 observer와의 충돌이 안생긴다.
		
		callback.onTaskAccountDischarged(this, bound);// ACC : UNAVAILABLE, TASK : UNAVAILABLE
	}

	@Override
	public void onAccountExceptionOccurred(Long bound) {// account occur - 다시 실행되어야 한다.
		Logger.printException("Exception occurred");

		callback.onTaskUnexpectedlyStopped(this, bound);// ACC : WORKING, TASK : UNAVAILABLE
	}

	@Override
	public void onAccountRangeDone() {// range done - 끝난 것.
		Logger.printException("Range done");
		
		account.updateStatus();// 다 썼으니까 refresh 한번 해준다.(working 상태가 아니게 만드는 의미도 있다.)
		
		callback.onTaskJobCompleted(this);// ACC : 모르고, TASK : DONE. BREAK;
	}
	
	@Override
	public void onAccountStoringFailed(Long bound) {
		Logger.printException("Storing failed");
		
		// 다른 데서라도 쓰이게 한다. working 이던 것들도 어차피 task가 꺼지므로 update되어야 한다. 하지만 곧 다른 task들도 마찬가지로 꺼질 것이다.
		account.updateStatus();
		
		callback.onTaskIncompletelyFinished(this, bound);
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

	public interface TaskCallback {
		void onTaskAccountDischarged(Task task, long bound);
		void onTaskUnexpectedlyStopped(Task task, long bound);
		void onTaskJobCompleted(Task task);
		void onTaskIncompletelyFinished(Task task, long bound);
	}
	
}
