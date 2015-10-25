package java.services.grep;
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
			if(list.getPagination().getNextMaxId().compareTo(from) < 0) {
				result = filterList(result, from);
				
				return result;
			} else {
				result = list.getData();
			}
			
			Pagination page = list.getPagination();
			MediaFeed nextList = instagram.getRecentMediaNextPage(page);
            
            while(true) {
            	// page 더이상 없는 경우
            	if(nextList.getPagination() == null) {
            		Logger.printException("Last page");
            		
            		callback.onAccountFinished();
            		
            		break;
            	}
            	
            	// query limit 다 쓴 경우
            	if(nextList.getRemainingLimitStatus() == 0) {
            		Logger.printException("Limit exceeded");
            		
            		callback.onAccountExceeded();
            		
            		break;
            	}
            	
            	// range check.
            	if(nextList.getPagination().getNextMaxTagId().compareTo(from) < 0) {
            		result.addAll(filterList(nextList.getData(), from));
            		
            		callback.onAccountCompleted();
            		
            		break;// 일반적으로 정상적인 exit route.
            	} else {
                    result.addAll(nextList.getData());
                    
                    page = nextList.getPagination();
                    nextList = instagram.getRecentMediaNextPage(page);
            	}
            }
		} catch (InstagramException e) {
			Logger.printException(e.getMessage());
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
	
	public interface AccountCallback {
		void onAccountFinished();// zero page
		void onAccountExceeded();// limit exceeded
		void onAccountCompleted();// range done
	}

}