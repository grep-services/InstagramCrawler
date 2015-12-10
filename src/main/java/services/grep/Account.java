package main.java.services.grep;
import java.util.Iterator;
import java.util.List;

import org.jinstagram.Instagram;
import org.jinstagram.auth.model.Token;
import org.jinstagram.entity.common.Pagination;
import org.jinstagram.entity.tags.TagInfoData;
import org.jinstagram.entity.tags.TagInfoFeed;
import org.jinstagram.entity.tags.TagMediaFeed;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;

import main.java.services.grep.Database.DatabaseCallback;

/**
 * 
 * instagram object 자체는 단일 account로부터 만들어지므로
 * 각 class가 갖고 있도록 한다.
 * 
 * 꼭 필요한 query method만 갖도록 한다.
 * 
 * @author marine1079
 * @since 151025
 *
 */

public class Account {

	public enum Status {// 단순히 boolean으로는 정확히 다룰 수가 없고, 그러면 꼬일 수가 있어서 이렇게 간다.
		UNAVAILABLE, WORKING, FREE;// 단순 교대기 때문에, reserved는 의미가 없다. 그냥 할당되면 그것으로 working인 것이다.
	}
	public Status status;
	
	int id;
	
	private final String clientId;// 현재는 확인용에밖에 쓸 일이 없다.
	private final String clientSecret;
	private final String accessToken;
	
	Instagram instagram;
	
	private static final int query_batch = 1000;
	private static final int database_batch = 10000;
	
	AccountCallback callback;

	public Account(String clientId, String clientSecret, String accessToken) {
		this.clientId = clientId;
		this.clientSecret = clientSecret;
		this.accessToken = accessToken;
		
		init();
	}
	
	public void init() {
		status = Status.UNAVAILABLE;
		
		Token token = new Token(accessToken, clientSecret);
		
		instagram = new Instagram(token);
	}
	
	// task에 할당될 때마다 바뀔 수 있다.
	public void setCallback(AccountCallback callback) {
		this.callback = callback;
	}
	
	public long getTagCount(String tag) {
		long count = 0;
		
		try {
			TagInfoFeed info = instagram.getTagInfo(tag);
			
			if(info != null) {// 사실 문제없어보이긴 하지만 확신할 수 없는 만큼 null check는 한다.
				TagInfoData data = info.getTagInfo();
				
				if(data != null) {
					count = data.getMediaCount();
				}
			}
		} catch (InstagramException e) {
			Logger.printException(e);
		}
		
		return count;
	}
	
	public void writeListToDB(List<MediaFeedData> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		/*
		 * thread로 돌기 때문에 callback은 여기의 결과와 상관없이 진행할 수밖에 없다.
		 * 그리고 여기에서는 list 쪼개고, thread들 start하도록 한다.
		 * 결국, 쓰는 과정의 thread에서 문제 생기면, 그것은 기록으로 끝날 수밖에 없다.
		 */
		// split, allocation, start
		// 일단 1개로 진행해본다.
		DatabaseCallback callback = ((DatabaseCallback) ((Task) this.callback).getCallback());
		
		Database database = new Database(list, callback);
		
		database.start();
		
		/*
		 * 주기적 write가 아닌만큼 wait해도 무방하다 생각
		 * 문제 있다면 없앤다.
		 */
		try {
			database.join();
		} catch (InterruptedException e) {
			Logger.printException(e);
		}
	}
	
	// TODO: 장기적으로, ARG들 이제 LONG으로 바꾼다. 굳이 STRING 할 필요 없다.
	// string으로 더 많이 쓰이며, null까지 들어갈 수 있는 관계로 이렇게 했다.
	public void getListFromTag(String tag, String from, String to) {
		List<MediaFeedData> result = null;
		
		try {
			// library가 object 구조를 좀 애매하게 해놓아서, 바로 loop 하기보다, 1 cycle은 직접 작성해주는 구조가 되었다.
			TagMediaFeed list = instagram.getRecentMediaTags(tag, null, to);// max도 null일 수 있다.(recent)
			
			/*
			 * page 관련되어서는 max, min의 null 여부를 두고 판단한다. min null인건 사실 zero page이고, max null인건 end of page라는 뜻이다.
			 * zero page는 뭐 굳이 따로 구분할 필요 없고, end of page인건 어차피 range done에 속한다.
			 * 따라서, null check부터 해서, end of page인지, range 초과했는지만 조사해주면 된다.
			 * 그리고 next max는 말그대로 next의 것이므로, 현재 list는 from안에 다 들어올 수도 있다.
			 * 참고로, hasnextpage가 안되는줄 알았는데 일단 되길래 쓴다. 이건 url 존재여부로 판단하는 것이다.
			 */
			if(!list.getPagination().hasNextPage() || Long.valueOf(list.getPagination().getNextMaxTagId()) < Long.valueOf(from)) {
				result = filterList(list.getData(), from);

				// 선 기록 후 보고, 그래야 차라리 기록 후 보고가 안되면 다시 덮어써지지, 반대로 되면 빈 공간이 생길 것.
				writeListToDB(result);
				
				callback.onAccountRangeDone();
				
				return;// result;
			} else {
				result = list.getData();
			}
			
			if(list.getRemainingLimitStatus() == 0) {// 다 되어도 0이면 return해야 한다.
				writeListToDB(result);
				
				callback.onAccountLimitExceeded(Long.valueOf(list.getPagination().getNextMaxTagId()));// range 바로 잡기 좋게 next max로...
        		
        		return;// result;
			}
			
			Pagination page = list.getPagination();
			MediaFeed nextList = instagram.getRecentMediaNextPage(page);
			
            while(true) {
            	// range check.
            	if(!nextList.getPagination().hasNextPage() || Long.valueOf(nextList.getPagination().getNextMaxTagId()) < Long.valueOf(from)) {
            		result.addAll(filterList(nextList.getData(), from));
            		
            		writeListToDB(result);

        			callback.onAccountRangeDone();
            		
            		break;// 일반적으로 정상적인 exit route.
            	} else {
                    result.addAll(nextList.getData());
                    
                    if(result.size() % query_batch == 0) {
                    	callback.onAccountQueried(query_batch);
                    }
                    
                    if(result.size() % database_batch == 0) {
                    	throw new Exception(String.format("<Account %d> Store and resuming.", id));
                    }
            	}
            	
            	// query limit 다 쓴 경우
            	if(nextList.getRemainingLimitStatus() == 0) {
            		writeListToDB(result);
            		
        			callback.onAccountLimitExceeded(Long.valueOf(nextList.getPagination().getNextMaxTagId()));
            		
            		break;
            	}
            	
                page = nextList.getPagination();
                nextList = instagram.getRecentMediaNextPage(page);
            }
		//} catch(InstagramException e) {
		} catch (Exception e) {// json malformed exception 등 예상치 못한 exception들도 더 있는 것 같다.
			Logger.printException(e);// 출력은 해준다.
			/*
			 * 여기는, exceeded 뿐만 아니라, 일반적인 ioexception 등 여러가지 올 수 있다.
			 * 판단 기준은 result뿐이며, 일반 list인 관계로 max id 같은 것 없다.
			 * result last item id로 range check 하고 callback에 item id -1로 bound 넘긴다.
			 * 물론 result null 및 empty check는 수반되어야 한다.
			 */
			if(result == null || result.isEmpty()) {
				callback.onAccountRangeDone();// 없어도 어쨌든 완료다.
			} else {// result에의 대입 자체가 from을 넘어선 assign을 하지 않기 때문에 그 check를 할 필요는 없고, 다만 bound를 알려주면 된다.
				writeListToDB(result);
				// from은 최소 0이고, id도 최소 0이기 때문에, 둘다 0이라면 위에서 filtered될 것이므로, 여기서 -1을 해도 음수가 될 일은 없다.
				callback.onAccountExceptionOccurred(Long.valueOf(result.get(result.size() - 1).getId().split("_")[0]) - 1);
			}
		}
	}
	
	public List<MediaFeedData> filterList(List<MediaFeedData> list, String bound) {
		for(Iterator<MediaFeedData> iterator = list.iterator(); iterator.hasNext();) {
			MediaFeedData item = iterator.next();
			
			// split 주의.
			if(Long.valueOf(item.getId().split("_")[0]) < Long.valueOf(bound)) {// 아마 오름차순일 것이므로 그냥 끝까지 해야 한다.
				iterator.remove();
			}
		}
		
		return list;
	}
	
	// 아마 0일 때는 exception 날 수도 있을 것 같다.
	private int getRateRemaining() {
		int remaining = -1;
		
		try {
			MediaFeed mediaFeed = instagram.getUserFeeds();
			
			if(mediaFeed != null) {// 혹시 모르니 해준다.
				remaining =  mediaFeed.getRemainingLimitStatus();// 여기서도 exception 날 수 있으니 값을 바로 return하지 않는다.
			}
		} catch(InstagramException e) {
			Logger.printException(e);
		}
		
		return remaining;
	}
	
	public void updateStatus() {
		if(status == Status.WORKING) {// working일 때는 안하는게 좋고, 이 때의 refresh는, work 끝나고 task가 직접 해준다.
			return;
		}
		
		int remaining = getRateRemaining();
		
		if(remaining != -1) {
			if(remaining < 5000 / 2) {
				status = Status.UNAVAILABLE;
			} else {
				status = Status.FREE;
			}
		}
	}
	
	public void setStatus(Status status) {
		this.status = status;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public void setAccountId(int id) {
		this.id = id;
	}
	
	public int getAccountId() {
		return id;
	}
	
	public interface AccountCallback {
		void onAccountLimitExceeded(Long bound);// limit exceeded
		void onAccountExceptionOccurred(Long bound);// exception occured
		void onAccountRangeDone();// range done
		void onAccountQueried(long size);
	}

}