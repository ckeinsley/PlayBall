package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import com.google.protobuf.ServiceException;

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
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			System.out.println("Master Not Running when connecting");
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			System.out.println("Zookeeper Connection Error when connecting");
		} catch (ServiceException e) {
			e.printStackTrace();
			System.out.println("Service Exception when connecting");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException when connecting");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("I CAUGHT AN ERROR. FIX THIS LATER");
		}
	}

	private void connect()
			throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
		Configuration config = HBaseConfiguration.create();
		config.addResource("hbase-site.xml");
		System.out.println("config = " + config);
//		config.set("hbase.zookeepr.quorum", "hadoop-m1.c.hadoop-play.ball.internal");
//		config.set("hbase.zookeeper.property.clientPort", "35.188.56.13:2181, 104.198.206.210:2181", "104.154.140.30:2181");
		HBaseAdmin.checkHBaseAvailable(config);
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
