package java.services.grep;
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

	public Logger() {
	}
	
	public static void printException(String msg) {
		System.out.println("Exception : " + msg);
	}

}
