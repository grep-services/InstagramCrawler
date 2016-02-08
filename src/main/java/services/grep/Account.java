package main.java.services.grep;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.Range;
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
	
	int id;
	
	private final String clientId;// 현재는 확인용에밖에 쓸 일이 없다.
	private final String clientSecret;
	private final String accessToken;
	
	Instagram instagram;
	
	private static final int BATCH = 10000;// 1개가 0.1KB정도라고 쳤을 때 1MB 정도가 되며, 30개의 acc면 30m 정도이다. 10만개 단위는 좀 많다.
	
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
	
	public long getLastMediaId(String tag) {
		long id = 0;
		
		try {
			TagMediaFeed list = instagram.getRecentMediaTags(tag, null, null);
			
			List<MediaFeedData> data = list.getData();
			
			if(data != null && !data.isEmpty()) {
				id = extractId(data.get(0).getId());
			}
		} catch (InstagramException e) {
			Logger.getInstance().printException(e);
		}
		
		return id;
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
			Logger.getInstance().printException(e);
		}
		
		return count;
	}
	
	/*
	 * 너무 복잡하면 힘들다.
	 * 여기서 하던 filtering은 밖으로 뺀다.
	 * 그리고 여기서 처리하던 limit, ioexception, 그외 exception도 밖으로 뺀다.
	 * db insertion도 밖으로 뺀다.
	 * 즉, 여기서는 무조건 값만 받고, 자연스럽게 또는 exception에 의해 return한다.
	 * 2차 exception은 애초에 여기서 바로 다시 시도하는 것이 아니기 때문에 고려할 필요 없다.
	 * 
	 * result, exception 정리를 위해 result exception throw로 return 단일화한다.
	 */
	public Result getListFromTag(String tag, Range<Long> range) {
		Result.Status status = Result.Status.Empty;// default는 not exception으로서 entire range travelled를 뜻한다.
		List<MediaFeedData> result = new ArrayList<MediaFeedData>();// 값 유지를 위해 공간은 만들어두어야 한다. TODO: 다시 확인.
		
		// TODO: RANGE NULL CHECK 일단 뺐는데, 필요할지 확인.
		long from = range.getMinimum();
		long to = range.getMaximum();
		
		try {
			// library가 object 구조를 좀 애매하게 해놓아서, 바로 loop 하기보다, 1 cycle은 직접 작성해주는 구조가 되었다.
			TagMediaFeed list = instagram.getRecentMediaTags(tag, null, String.valueOf(to));
			Pagination page = list.getPagination();
			List<MediaFeedData> data = list.getData();
			
			if(!addFilteredData(result, data, from, to)) {// filter가 안되어야만 다음으로 넘어가고, 아니면 그냥 그대로 끝이다.
				if(page.hasNextPage()) {
					MediaFeed nextList = instagram.getRecentMediaNextPage(page);
					Pagination nextPage = nextList.getPagination();
					List<MediaFeedData> nextData = nextList.getData();
					
		            while(true) {
		            	if(!addFilteredData(result, nextData, from, to)) {// filter가 안되어야만 다음으로 넘어가고, 아니면 그냥 그대로 끝이다.
		            		if(result != null && result.size() > BATCH) {
		            			throw new Exception();//TODO: 밑으로 내려가는지 확인.
		            		}
		            		
			    			if(nextPage.hasNextPage()) {
				                nextList = instagram.getRecentMediaNextPage(nextPage);
				                nextPage = nextList.getPagination();
				                nextData = nextList.getData();
			    			} else {
			    				break;
			    			}
		            	} else {// if 자체가 while 안에서 실행되어야 되기 때문에 이렇게 else에서 break 걸어줘야 한다.
		            		break;
		            	}
		            }
				}
			}
		} catch (Exception e) {// 아래의 단계를 거치게 해야 task loop에서의 처리가 깔끔해진다.
			status = Result.Status.Normal;// 기본적으로 normal.
			
			if(e instanceof InstagramException) {// 만약 insta exception이라면 exceed로 간주.
				status = Result.Status.Exceed;
			}
			
			if(result != null && !result.isEmpty()) {// 그런데 만약 full travel이라면 empty로 간주.
				long id = extractId(result.get(result.size() - 1).getId());
				
				if(id == range.getMinimum()) {
					status = Result.Status.Empty;
				}
			}
		}
		
		return new Result(status, result);
	}
	
	/*
	 * getlist method의 전체적인 내용을 줄이고
	 * 특히 null check 및 boolean check 관련 내용으로 인한 복잡화 방지를 위한 method.
	 * return값은, 마지막이라는 의미이며, 이 값이 쓰이는 곳도 있다.(while)
	 */
	public boolean addFilteredData(List<MediaFeedData> list, List<MediaFeedData> data, long from, long to) {
		if(data != null && !data.isEmpty()) {
			boolean lower = extractId(data.get(data.size() - 1).getId()) < from;
			boolean upper = extractId(data.get(0).getId()) > to;
			
			for(Iterator<MediaFeedData> iterator = data.iterator(); iterator.hasNext();) {
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
			
			list.addAll(data);
			
			return lower || upper;
		} else {
			return true;
		}
	}
	
	public long extractId(String string) {
		return Long.valueOf(string.split("_")[0]);
	}
	
	// 아마 0일 때는 exception 날 수도 있을 것 같다.
	public int getRateRemaining() {
		int remaining = -1;
		
		try {
			MediaFeed mediaFeed = instagram.getUserFeeds();
			
			if(mediaFeed != null) {// 혹시 모르니 해준다.
				remaining =  mediaFeed.getRemainingLimitStatus();// 여기서도 exception 날 수 있으니 값을 바로 return하지 않는다.
			}
		} catch(InstagramException e) {
			Logger.getInstance().printException(e);
		}
		
		return remaining;
	}
	
	public void setAccountId(int id) {
		this.id = id;
	}
	
	public int getAccountId() {
		return id;
	}
	
}