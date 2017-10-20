package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

public class Main {

	public void run() {
		attemptConnection();
		Scanner in = new Scanner(System.in);
		while (true) {
			System.out.print("PlayBall > ");
			String line = in.nextLine();
			if (line.equals("exit") || line.equals("quit")) {
				System.out.println("Exiting");
				in.close();
				exit();
			}
		}
	}

	private void attemptConnection() {
		try {
			System.out.println("Attempting Connection...");
			connect();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException when connecting");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("I CAUGHT AN ERROR. FIX THIS LATER");
		} finally {
			System.out.println("------------------FINALLY---------------");
		}
	}

	private void connect() throws IOException {
		System.out.println("Constructing Config");
		Configuration config = HBaseConfiguration.create();
		System.out.println("Adding Resource");
		config.addResource("hbase-site.xml");
		System.out.println("Creating Connection");
		Connection conn = ConnectionFactory.createConnection(config);
		System.out.println("Getting Admin");
		Admin admin = conn.getAdmin();
		System.out.println("Tables Should be Soon");
		System.out.println("Tables?  " + Arrays.toString(admin.listTableNames()));
	}

	public void exit() {
		// CleanUp
		System.exit(0);
	}

	public static void main(String[] args) {
		initLogger();
		Main main = new Main();
		main.run();
	}

	public static void initLogger() {
		try {
			String filePath = "logPlayBall.log";
			PatternLayout layout = new PatternLayout("%-5p %d %m%n");
			RollingFileAppender appender = new RollingFileAppender(layout, filePath);
			appender.setName("myFirstLog");
			appender.setMaxFileSize("1MB");
			appender.activateOptions();
			Logger.getRootLogger().addAppender(appender);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
