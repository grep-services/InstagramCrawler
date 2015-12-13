package main.java.services.grep;
import java.util.Iterator;
import java.util.List;

import main.java.services.grep.Database.DatabaseCallback;

import org.jinstagram.Instagram;
import org.jinstagram.auth.model.Token;
import org.jinstagram.entity.common.Pagination;
import org.jinstagram.entity.tags.TagInfoData;
import org.jinstagram.entity.tags.TagInfoFeed;
import org.jinstagram.entity.tags.TagMediaFeed;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;

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
	
	public long getLastMediaId(String tag) {
		long id = 0;
		
		try {
			TagMediaFeed list = instagram.getRecentMediaTags(tag, null, null);
			
			List<MediaFeedData> data = list.getData();
			
			if(data != null && !data.isEmpty()) {
				id = extractId(data.get(0).getId());
			}
		} catch (InstagramException e) {
			Logger.printException(e);
		}
		
		return id;
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
	
	public void getListFromTag(String tag, long from, long to) {
		List<MediaFeedData> result = null;
		
		try {
			// library가 object 구조를 좀 애매하게 해놓아서, 바로 loop 하기보다, 1 cycle은 직접 작성해주는 구조가 되었다.
			TagMediaFeed list = instagram.getRecentMediaTags(tag, null, String.valueOf(to));
			
			Pagination page = list.getPagination();
			
			List<MediaFeedData> data = list.getData();
			
			/*
			 * 1. empty check.
			 * loop에서는 없겠지만 여기선 있을 수 있다.
			 */
			if(data == null || data.isEmpty()) {
				callback.onAccountRangeDone();
				
				return;
			}
			
			/*
			 * 2. boundary check.
			 * page가 더 있든 말든 넘어가면 끝이다.
			 * TODO: 다만, upper는 아직 확신하지 못한다.
			 * 그리고 filter한 결과는 보존해준다.
			 */
			boolean lower = extractId(data.get(data.size() - 1).getId()) < from;
			boolean upper = extractId(data.get(0).getId()) > to;
			
			if(lower || upper) {
				result = filterList(data, lower, upper, from, to);
				
				writeListToDB(result);
				
				callback.onAccountRangeDone();
				
				return;
			} else {
				result = data;
			}
			
			/*
			 * 3. nextpage check.
			 * not empty, boundary 괜찮고 해도, 마지막 page는 항상 있고, 거기서 끝내야 한다.
			 * 이미 filtered이므로 그냥 쓰고 return하면 된다.
			 */
			if(!page.hasNextPage()) {
				// 선 기록 후 보고, 그래야 차라리 기록 후 보고가 안되면 다시 덮어써지지, 반대로 되면 빈 공간이 생길 것.
				writeListToDB(result);
				
				callback.onAccountRangeDone();
				
				return;
			}
			
			/*
			 * 4. limit check
			 * last page까지 아닌 일반적인 page라 하더라도
			 * 더이상 limit가 없을 때는 쓰고 return.
			 * 특히, nextmax가 not null이다. 왜냐하면 null이라면 last page로서 위에서 걸렸을 것이기 때문이다.
			 */
			if(list.getRemainingLimitStatus() == 0) {
				writeListToDB(result);
				
				callback.onAccountLimitExceeded(Long.valueOf(page.getNextMaxTagId()));// range 바로 잡기 좋게 next max로...
        		
        		return;// result;
			}
			
			MediaFeed nextList = instagram.getRecentMediaNextPage(page);
			Pagination nextPage = nextList.getPagination();
			List<MediaFeedData> nextData = nextList.getData();
			boolean nextLower = extractId(nextData.get(nextData.size() - 1).getId()) < from;
			boolean nextUpper = extractId(nextData.get(0).getId()) > to;
			
            while(true) {
    			/*
    			 * 1. boundary check.
    			 * page가 더 있든 말든 넘어가면 끝이다.
    			 * TODO: 다만, upper는 아직 확신하지 못한다.
    			 * 그리고 filter한 결과는 보존해준다.
    			 */
    			
    			if(nextLower || nextUpper) {
    				result.addAll(filterList(nextData, nextLower, nextUpper, from, to));
    				
    				writeListToDB(result);
    				
    				callback.onAccountRangeDone();
    				
    				return;
    			} else {
    				result.addAll(nextData);
    				
                    if(result.size() % query_batch == 0) {
                    	callback.onAccountQueried(query_batch);
                    }
                    
                    if(result.size() % database_batch == 0) {
                    	throw new Exception(String.format("<Account %d> Store and resuming.", id));
                    }
    			}
    			
    			/*
    			 * 2. nextpage check.
    			 * not empty, boundary 괜찮고 해도, 마지막 page는 항상 있고, 거기서 끝내야 한다.
    			 * 이미 filtered이므로 그냥 쓰고 return하면 된다.
    			 */
    			if(!nextPage.hasNextPage()) {
    				// 선 기록 후 보고, 그래야 차라리 기록 후 보고가 안되면 다시 덮어써지지, 반대로 되면 빈 공간이 생길 것.
    				writeListToDB(result);
    				
    				callback.onAccountRangeDone();
    				
    				return;
    			}
    			
    			/*
    			 * 3. limit check
    			 * last page까지 아닌 일반적인 page라 하더라도
    			 * 더이상 limit가 없을 때는 쓰고 return.
    			 * 특히, nextmax가 not null이다. 왜냐하면 null이라면 last page로서 위에서 걸렸을 것이기 때문이다.
    			 */
            	if(nextList.getRemainingLimitStatus() == 0) {
            		writeListToDB(result);
            		
        			callback.onAccountLimitExceeded(Long.valueOf(nextPage.getNextMaxTagId()));
            		
            		break;
            	}
            	
                nextList = instagram.getRecentMediaNextPage(nextPage);
                nextPage = nextList.getPagination();
                nextData = nextList.getData();
    			nextLower = extractId(nextData.get(nextData.size() - 1).getId()) < from;
    			nextUpper = extractId(nextData.get(0).getId()) > to;
            }
		} catch (Exception e) {// json malformed exception 등 예상치 못한 exception들도 더 있는 것 같다.
			Logger.printException(e);// 출력은 해준다.
			
			/*
			 * 여기에서도 위의 check condition들 피해갈 자격은 없다.
			 * 하지만 다 할 필요도 없다.
			 * 필요에 맞게, 그리고 bound로 귀결해서 정리해준다.
			 */
			
			long bound;
			
			try {
				// 1. empty check.
				if(result == null || result.isEmpty()) {
					bound = to;
					
					callback.onAccountExceptionOccurred(bound);
					
					return;
				}
				
				/*
				 * 2. boundary check.
				 * 여기서도 필요는 하다.
				 * TODO: 다만, upper는 아직 확신하지 못한다.
				 * 그리고 filter한 결과는 보존해준다.
				 */
				boolean lower = extractId(result.get(result.size() - 1).getId()) < from;
				boolean upper = extractId(result.get(0).getId()) > to;
				
				if(lower || upper) {
					bound = from;
					
					result = filterList(result, lower, upper, from, to);
					
					writeListToDB(result);
					
					callback.onAccountExceptionOccurred(bound);
					
					return;
				}
				
				/*
				 * 3. normal
				 * page, limit check는 할 수 없다.
				 * 그냥 write하고
				 * bound 구해서 return.
				 */
				bound = extractId(result.get(result.size() - 1).getId()) - 1;
				
				writeListToDB(result);
				
				// 그래도 bound가 from 아래로 떨어질 수 있으므로 처리해준다.
				callback.onAccountExceptionOccurred(bound < from ? from : bound);
				
				return;
			} catch (Exception e2) {
				/*
				 * 그래도 또 난다면, 여기서는 그냥 database에서처럼 해결불능 message를 낸다.
				 * account를 가만히 두면 stop된 후 observer에 의해 재시작될 것이다.
				 * 하지만 db에 제대로 쓴 것도 아니므로 resize를 명확히 해주기가 힘들다.
				 * 따라서 message 출력 후 차라리 task를 종료시키도록 resize해준다.
				 */
				Logger.printMessage(String.format("<Account %d> : error occurred.", id));
				
				bound = from;
				
				callback.onAccountExceptionOccurred(bound);
			}
		}
	}
	
	public long extractId(String string) {
		return Long.valueOf(string.split("_")[0]);
	}
	
	public List<MediaFeedData> filterList(List<MediaFeedData> list, boolean lower, boolean upper, long from, long to) {
		for(Iterator<MediaFeedData> iterator = list.iterator(); iterator.hasNext();) {
			MediaFeedData item = iterator.next();
			
			if(extractId(item.getId()) < from) {
				if(lower) {
					iterator.remove();
				}
			} else if(extractId(item.getId()) > to) {
				if(upper) {
					iterator.remove();
				}
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
		void onAccountLimitExceeded(long bound);// limit exceeded
		void onAccountExceptionOccurred(long bound);// exception occured
		void onAccountRangeDone();// range done
		void onAccountQueried(long size);
	}

}