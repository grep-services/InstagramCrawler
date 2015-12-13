package main.java.services.grep;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
	private String sql = "Insert into \"Instagram\" (media_id, link) values (?,?)";
	private PreparedStatement preparedStatement = null;
	private static final int batch = 1000;
	
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
				preparedStatement.setLong(1, extractId(item.getId()));
				preparedStatement.setString(2, item.getLink());
				
				preparedStatement.addBatch();
				
				count++;
				
				if(count % batch == 0 || count == list.size()) {
					int written = preparedStatement.executeBatch().length;// 일단 max_int까지 안가므로 large로 할 필요 없는 것 같다.
					index += written;
					
					//Logger.printProgress(written);
					callback.onDatabaseWritten(written);
				}
			}
		} catch(SQLException e) {
			Logger.printException(e);
			Logger.printException(e.getNextException());
		}
		
		if(index < list.size()) {
			/*
			 * account는 join해뒀기 때문에 다른 곳에 할당되지는 않을 것이다.
			 * 하지만 당장 여기서 난 exception 결과 bound 등을 다시 callback으로 account에 보낸다 해도
			 * 그것을 다시 resize 대상으로 잡고 하기에는 single range의 한계 및 복잡성 증가 문제가 있다.
			 * 일단 지금으로서는 database writting에서 나는 exception은 기록을 해두고 남겨두는 것으로 끝낸다.
			 * 어쨌든 현재는 writting 되든 말든 이미 retrive한 data에 대해서는 resizing하면서 진행하므로
			 * data상에서의 문제는 있겠지만(하지만 기록을 해두므로 괜찮다.) 진행상의 문제는 없을 것이다. 
			 */
			Logger.printMessage(String.format("Database : writing failed. from %d to %d", extractId(list.get(list.size() - 1).getId()), extractId(list.get(index).getId())));
		}
		
		release();
	}
	
	public long extractId(String id) {
		return Long.valueOf(id.split("_")[0]);
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
