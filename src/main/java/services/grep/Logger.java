package main.java.services.grep;
/**
 * 
 * 이 시스템에 맞게 특화된 logger.
 * 주 목적은 crawler monitoring하는 사람이 알아보기 쉽게 하는 것이다.
 * 
 * @author marine1079
 * @since 151025
 *
 */

public class Logger {

	private static int progress = 0;
	private static long startTime = 0;
	
	public Logger() {
	}
	
	public static void printException(Exception e) {
		System.out.println("Exception : " + e.getMessage());
	}
	
	public static void printMessage(String msg) {
		System.out.println("Message : " + msg);
	}
	
	// 여기서 다 하기는 힘들다. main과 연결하도록 한다.
	public synchronized static void printProgress(int written) {
		if(progress == 0) {
			startTime = System.currentTimeMillis();
		}
		
		progress += written;
		
		System.out.println("Progress : " + progress);
	}

}
