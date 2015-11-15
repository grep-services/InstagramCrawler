package main.java.services.grep;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jinstagram.entity.users.feed.MediaFeedData;

public class Database {

	private static final Database database = new Database();
	
	private static final String driver = "org.postgresql.Driver";
	private static final String url = "jdbc:postgresql:grep";
	private static final String user = "postgres";
	private static final String password = "1735ranger";
	
	private Connection connection = null;
	private Statement statement = null;
	private String sql = "Insert into Instagram (sid, link) values (?,?)";
	private PreparedStatement preparedStatement = null;
	private static final int batch = 1000;
	
	private Database() {
	}
	
	public static Database getInstance() {
		return database;
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
	
	//TODO: THREAD SAFE 고민해봐야 한다. 정 안되면, 그냥 BATCH SYNC하고 마지막에 EXECUTE하는 쪽으로 간다.
	public int write(List<MediaFeedData> list) {
		if(list == null || list.isEmpty()) {
			return 0;
		}
		
		int count = 0, index = 0;
		
		try {
			for(MediaFeedData item : list) {
				preparedStatement.setLong(0, Long.valueOf(item.getId()));
				preparedStatement.setString(1, item.getLink());
				
				preparedStatement.addBatch();
				
				count++;
				
				if(count % batch == 0 || count == list.size() - 1) {
					int written = preparedStatement.executeBatch().length;// 일단 max_int까지 안가므로 large로 할 필요 없는 것 같다.
					index += written;
					
					Logger.printProgress(written);
				}
			}
		} catch(SQLException e) {
			Logger.printException(e);
		}
		
		return index;// select로 확인하자니, 속도 느릴 수 있을 것 같아서 일단 이렇게 return값 size로 해본다.
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

}
