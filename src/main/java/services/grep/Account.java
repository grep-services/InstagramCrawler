package main.java.services.grep;
import java.util.Iterator;
import java.util.List;

import org.jinstagram.Instagram;
import org.jinstagram.auth.model.Token;
import org.jinstagram.entity.common.Pagination;
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
	
	private final String clientId;// 현재는 확인용에밖에 쓸 일이 없다.
	private final String clientSecret;
	private final String accessToken;
	
	Instagram instagram;
	
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
	
	// string으로 더 많이 쓰이며, null까지 들어갈 수 있는 관계로 이렇게 했다.
	public List<MediaFeedData> getListFromTag(String tag, String from, String to) {
		List<MediaFeedData> result = null;
		
		try {
			// library가 object 구조를 좀 애매하게 해놓아서, 바로 loop 하기보다, 1 cycle은 직접 작성해주는 구조가 되었다.
			TagMediaFeed list = instagram.getRecentMediaTags(tag, null, to);// max도 null일 수 있다.(recent)
			
			// 첫 list로 받긴 하지만, 처음부터도 filtering을 해야 한다. 만약 filtered되면, 바로 return한다.
			if(list.getPagination() == null) {//TODO: 이게 단1개의 PAGE라도 없으면 나오는지, 마지막에 도달하면 나오는지 CHECK... -> 1개라도 없을때는 아닌듯. max가 아무리 커도 알아서 맞춰준다.
        		callback.onAccountPageEnd();
        		
        		return null;
			}
			
			if(list.getPagination().getNextMaxId().compareTo(from) < 0) {
				result = filterList(result, from);

        		callback.onAccountRangeDone();
				
				return result;
			} else {
				result = list.getData();
			}
			
			if(list.getRemainingLimitStatus() == 0) {// 다 되어도 0이면 return해야 한다.
        		callback.onAccountLimitExceeded(Long.valueOf(list.getPagination().getNextMaxId()));// range 바로 잡기 좋게 next max로...
        		
        		return result;
			}
			
			Pagination page = list.getPagination();
			MediaFeed nextList = instagram.getRecentMediaNextPage(page);
            
            while(true) {
            	// page 더이상 없는 경우
            	if(nextList.getPagination() == null) {
            		callback.onAccountPageEnd();
            		
            		break;
            	}
            	
            	// range check.
            	if(nextList.getPagination().getNextMaxTagId().compareTo(from) < 0) {
            		result.addAll(filterList(nextList.getData(), from));
            		
            		callback.onAccountRangeDone();
            		
            		break;// 일반적으로 정상적인 exit route.
            	} else {
                    result.addAll(nextList.getData());
            	}
            	
            	// query limit 다 쓴 경우
            	if(nextList.getRemainingLimitStatus() == 0) {
            		callback.onAccountLimitExceeded(Long.valueOf(nextList.getPagination().getNextMaxId()));
            		
            		break;
            	}
            	
                page = nextList.getPagination();
                nextList = instagram.getRecentMediaNextPage(page);
            }
		} catch (InstagramException e) {
			//TODO: 분명히, 애초에 LIMIT 0인 것은 여기로 올 수 있을 것 같다. 여기서도 CALLBACK 처리되게 해줘야 한다.
			Printer.printException(e.getMessage());
		}
		
		return result;
	}
	
	public List<MediaFeedData> filterList(List<MediaFeedData> list, String bound) {
		for(Iterator<MediaFeedData> iterator = list.iterator(); iterator.hasNext();) {
			MediaFeedData item = iterator.next();
			
			if(item.getId().compareTo(bound) < 0) {// 아마 오름차순일 것이므로 그냥 끝까지 해야 한다.
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
		} catch (InstagramException e) {
			//TODO: 분명히, 애초에 LIMIT 0인 것은 여기로 올 수 있을 것 같다. 여기서도 CALLBACK 처리되게 해줘야 한다.
			Printer.printException("LIMIT" + e.getMessage());
		}
		
		return remaining;
	}
	
	public void updateStatus() {
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
	
	public interface AccountCallback {
		void onAccountPageEnd();// zero page
		void onAccountLimitExceeded(Long bound);// limit exceeded
		void onAccountRangeDone();// range done
	}

}