package main.java.services.grep;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import org.jinstagram.entity.common.Images;
import org.jinstagram.entity.common.Location;
import org.jinstagram.entity.users.feed.MediaFeedData;

/**
 * 
 * 일단 입력량을 최대한 빠르게 분산시킬 수 있도록
 * thread extends하고 init, release 등 내부에서 많이 해준다.(어차피 다른 곳들도 비슷하긴 하다.)
 * 
 * @author Michael
 * @since 151116
 *
 */

public class Database extends Thread {

	private static final String driver = "org.postgresql.Driver";
	private static final String url = "jdbc:postgresql:grep";
	private static final String user = "postgres";
	private static final String password = "1735ranger";
	
	private Connection connection = null;
	private Statement statement = null;
	private String sql = "Insert into \"Instagram\" (media_id, created_time, link_url, image_url, likes_count, text, comments_count, user_id, user_name, location_name, latitude, longitude) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private PreparedStatement preparedStatement = null;
	
	private List<MediaFeedData> list = null;
	
	DatabaseCallback callback;
	
	public Database(List<MediaFeedData> list, DatabaseCallback callback) {
		setDaemon(true);
		
		this.list = list;
		this.callback = callback;
		
		init();
	}
	
	public void init() {
		try {
			Class.forName(driver);
			
			connection = DriverManager.getConnection(url, user, password);
			statement = connection.createStatement();
			preparedStatement = connection.prepareStatement(sql);
		} catch(ClassNotFoundException e) {
			Logger.getInstance().printException(e);
		} catch(SQLException e) {
			Logger.getInstance().printException(e);
		}
	}
	
	@Override
	public void run() {
		if(list == null || list.isEmpty()) {
			return;
		}
		
		int written = 0;
		
		try {
			for(MediaFeedData item : list) {
				preparedStatement.setLong(1, extractId(item.getId()));
				preparedStatement.setTimestamp(2, extractTimestamp(item.getCreatedTime()));// not milli but sec
				preparedStatement.setString(3, item.getLink());
				preparedStatement.setString(4, extractImageUrl(item));// not null but may not be full
				preparedStatement.setInt(5, item.getLikes().getCount());// not null
				preparedStatement.setString(6, extractText(item));// caption can be null
				preparedStatement.setInt(7, item.getComments().getCount());// not null
				preparedStatement.setLong(8, Long.valueOf(item.getUser().getId()));// should be long
				preparedStatement.setString(9, item.getUser().getUserName());// not null
				preparedStatement.setString(10, extractLocation(item.getLocation()));// location can be null
				preparedStatement.setString(11, extractLatitude(item.getLocation()));// location can be null
				preparedStatement.setString(12, extractLongitude(item.getLocation()));// location can be null
				
				preparedStatement.addBatch();
			}
			
			written = preparedStatement.executeBatch().length;// 일단 max_int까지 안가므로 large로 할 필요 없는 것 같다.

			callback.onDatabaseWritten(written);
		} catch(SQLException e) {
			Logger.getInstance().printException(e);
			Logger.getInstance().printException(e.getNextException());
		}
		
		if(written < list.size()) {
			/*
			 * 일단 지금으로서는 database writting에서 나는 exception은 기록을 해두고 남겨두는 것으로 끝낸다.
			 * 어쨌든 현재는 writting 되든 말든 이미 retrive한 data에 대해서는 resizing하면서 진행하므로
			 * data상에서의 문제는 있겠지만(하지만 기록을 해두므로 괜찮다.) 진행상의 문제는 없을 것이다. 
			 */
			Logger.getInstance().printMessage(String.format("<Database> Writing failed. from %d to %d remains.", extractId(list.get(list.size() - 1).getId()), extractId(list.get(written).getId())));
		}
		
		release();
	}
	
	public long extractId(String id) {
		return Long.valueOf(id.split("_")[0]);
	}
	
	public Timestamp extractTimestamp(String timeString) {
		return new Timestamp(Long.valueOf(timeString) * 1000);
	}
	
	public String extractText(MediaFeedData item) {
		return item.getCaption() != null ? item.getCaption().getText() : null;
	}
	
	// image 여러개 저장하기에는 좀 골치아프다. 일단 한개만 한다. 어차피 무조건 있다.
	public String extractImageUrl(MediaFeedData item) {
		String url = null;
		
		Images images = item.getImages();
		
		if(images.getStandardResolution() != null) {
			url = images.getStandardResolution().getImageUrl();
		} else if(images.getLowResolution() != null) {
			url = images.getLowResolution().getImageUrl();
		} else if(images.getThumbnail() != null) {
			url = images.getThumbnail().getImageUrl();
		}
		
		return url;
	}
	
	public String extractLocation(Location location) {
		return location != null ? location.getName() : null;
	}
	
	public String extractLatitude(Location location) {
		return location != null ? String.valueOf(location.getLatitude()) : null;
	}
	
	public String extractLongitude(Location location) {
		return location != null ? String.valueOf(location.getLongitude()) : null;
	}
	
	public void release() {
		try {
			preparedStatement.close();
			statement.close();
			connection.close();
		} catch(SQLException e) {
			Logger.getInstance().printException(e);
		}
	}
	
	public interface DatabaseCallback {
		void onDatabaseWritten(int written);// 일단 amount만 가지고 해본다.
	}

}
