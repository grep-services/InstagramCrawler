package main.java.services.grep;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

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

	private static final Logger instance = new Logger();
	
	private BufferedWriter writer;
	
	// 이런 것들은 다음에 factory형식으로 변환.
	private static boolean CONSOLE = true;
	private static boolean FILE = true;
	
	public static Logger getInstance() {
		return instance;
	}
	
	private Logger() {
		init();
	}
	
	private void init() {
		try {
			writer = new BufferedWriter(new FileWriter("output"));
		} catch (IOException e) {
			System.out.println("Exception : " + e.getMessage());
		}
	}
	
	public void release() {
		try {
			writer.close();
			
			FILE = false;
		} catch (IOException e) {
			System.out.println("Exception : " + e.getMessage());
		}
	}
	
	public void printException(Exception e) {
		String string = "Exception : " + e.getMessage();
		
		if(CONSOLE) {
			System.out.println(string);
		}
		
		if(FILE) {
			if(writer != null) {
				try {
					writer.write(string + '\n');
				} catch (IOException e1) {
					System.out.println("Exception : " + e1.getMessage());// prevent recursive
				}
			}
		}
	}
	
	public void printMessage(String msg) {
		String string = "Message : " + msg;
		
		if(CONSOLE) {
			System.out.println(string);
		}
		
		if(FILE) {
			if(writer != null) {
				try {
					writer.write(string + '\n');
				} catch (IOException e1) {
					System.out.println("Exception : " + e1.getMessage());// prevent recursive
				}
			}
		}
	}
	
	public void printMessage(String format, Object... args) {
		printMessage(String.format(format, args));
	}

}