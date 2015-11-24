package main.java.services.grep;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
	private String sql = "Insert into Instagram (sid, link) values (?,?)";
	private PreparedStatement preparedStatement = null;
	private static final int batch = 1000;
	
	private List<MediaFeedData> list = null;
	
	DatabaseCallback callback;
	
	public Database(List<MediaFeedData> list, DatabaseCallback callback) {
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
			Logger.printException(e);
		} catch(SQLException e) {
			Logger.printException(e);
		}
	}
	
	@Override
	public void run() {
		if(list == null || list.isEmpty()) {
			return;
		}
		
		int count = 0, index = 0;
		
		try {
			for(MediaFeedData item : list) {
				preparedStatement.setLong(1, Long.valueOf(item.getId()));
				preparedStatement.setString(2, item.getLink());
				
				preparedStatement.addBatch();
				
				count++;
				
				if(count % batch == 0 || count == list.size() - 1) {
					int written = preparedStatement.executeBatch().length;// 일단 max_int까지 안가므로 large로 할 필요 없는 것 같다.
					index += written;
					
					//Logger.printProgress(written);
					callback.onDatabaseWritten(written);
				}
			}
		} catch(SQLException e) {
			Logger.printException(e);
		}
		
		if(index < list.size()) {
			// 이제 이미 account는 다른데에 쓰이고 있을수도 있는만큼, 여기서의 exception은 여기서 처리한다.
			Logger.printMessage(String.format("Database : writing failed. from %s to %s", list.get(list.size() - 1).getId(), list.get(index).getId()));
		}
		
		release();
	}
	
	public void release() {
		try {
			preparedStatement.close();
			statement.close();
			connection.close();
		} catch(SQLException e) {
			Logger.printException(e);
		}
	}
	
	public interface DatabaseCallback {
		void onDatabaseWritten(int written);// 일단 amount만 가지고 해본다.
	}

}
