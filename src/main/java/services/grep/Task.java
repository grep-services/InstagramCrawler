package main.java.services.grep;

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
	
	public boolean isWorking = false;
	
	String tag;
	Account account;
	Range<Long> range;
	TaskCallback callback;
	
	public Task(String tag, Account account, Range<Long> range, TaskCallback callback) {
		this.tag = tag;
		this.account = account;
		this.range = range;
		this.callback = callback;
		
		status = Status.UNAVAILABLE;
	}
	
	// constructor 이외에도 set 될 일들 많다.
	public void setAccount(Account account) {
		if(this.account != null) {// 이미 쓰던게 있었다면
			this.account.updateStatus();// status 정리해준다.
		}
		
		account.setStatus(Account.Status.WORKING);
		
		this.account = account;
	}
	
	public Account getAccount() {
		return account;
	}

	@Override
	public void run() {
		// 결과는 callback으로 넘어가고, range는 사실 list의 min값을 확인해서 쓰면 된다.
		List<MediaFeedData> list = account.getListFromTag(tag, String.valueOf(range.getMinimum()), String.valueOf(range.getMaximum()));
		
		// db에 저장
	}

	@Override
	public void onAccountPageEnd() {// zero page - 어차피 끝난건 마찬가지이다. 다만, 끝난게 아니라 원래 없던 것일수도.
		Printer.printException("Last page");
		
		account.updateStatus();// 다 썼으니까 refresh 한번 해준다.(working 상태가 아니게 만드는 의미도 있다.)
		account = null;// 굳이 할 필요는 없지만...
		
		callback.onTaskJobCompleted(this);
	}

	@Override
	public void onAccountLimitExceeded(Long bound) {// limit exceeded - 새 account를 요청해야 한다.
		Printer.printException("Limit exceeded");
		
		range = Range.between(range.getMinimum(), bound);// range 재정산.
		
		account.setStatus(Account.Status.UNAVAILABLE);
		account = null;// 굳이 할 필요는 없지만...
		
		callback.onTaskAccountDischarged(this);
	}

	@Override
	public void onAccountRangeDone() {// range done - 끝난 것.
		Printer.printException("Range done");
		
		account.updateStatus();// 다 썼으니까 refresh 한번 해준다.(working 상태가 아니게 만드는 의미도 있다.)
		account = null;// 굳이 할 필요는 없지만...
		
		callback.onTaskJobCompleted(this);
	}
	
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public Status getStatus() {
		return status;
	}

	public interface TaskCallback {
		void onTaskAccountDischarged(Task task);
		void onTaskJobCompleted(Task task);
	}
	
}
