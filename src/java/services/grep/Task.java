package java.services.grep;

import java.services.grep.Account.AccountCallback;

import org.apache.commons.lang3.Range;

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

	public boolean isWorking = false;
	
	String tag;
	Account account;
	Range<Long> range;
	
	public Task(String tag, Account account, Range<Long> range) {
		this.tag = tag;
		this.account = account;
		this.range = range;
	}
	
	// constructor 이외에도 set 될 일들 많다.
	public void setAccount(Account account) {
		this.account = account;
	}

	@Override
	public void run() {
		// 결과는 callback으로 넘어가고, range는 사실 list의 min값을 확인해서 쓰면 된다.
		account.getListFromTag(tag, String.valueOf(range.getMinimum()), String.valueOf(range.getMaximum()));
	}

	@Override
	public void onAccountFinished() {
		
	}

	@Override
	public void onAccountExceeded() {

	}

	@Override
	public void onAccountCompleted() {
		
	}
	
}
