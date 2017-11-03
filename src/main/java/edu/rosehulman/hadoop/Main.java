package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Main {

	private Connection conn;
	private Configuration config;
	public static boolean debug;
	private Searcher searcher;
	private PlayerStatsFinder playerStats;
	private TeamStatsFinder teamStats;

	public Main(boolean debugMode) {
		debug = debugMode;
	}

	public void run() {
		attemptConnection();
		Scanner in = new Scanner(System.in);
		while (true) {
			System.out.print("PlayBall > ");
			String line = in.nextLine();
			if (line.startsWith("exit") || line.startsWith("quit")) {
				in.close();
				exit();
			} else if (line.startsWith("search")) {
				search(line);
			} else if (line.startsWith("play")) {
				searcher.displayNextPlay();
			} else if (debug && line.startsWith("show tables")) {
				printTables();
			} else if (line.startsWith("PlayerStats")) {
				searchPlayerStats(line);
			} else if (line.startsWith("Player Stats")) {
				searchPlayerStats(line);
			} else if (line.startsWith("TeamStats")) {
				searchTeamStats(line);
			} else if (line.startsWith("Team Stats")) {
				searchTeamStats(line);
			} else if (line.startsWith("help")) {
				printHelp();
			} else {
				System.out.println("Unrecognized command: " + line);
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
		}
	}

	private void connect() throws IOException {
		System.out.println("Constructing Config");
		config = HBaseConfiguration.create();
		config.addResource("hbase-site.xml");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");

		System.out.println("Creating Connection");
		conn = ConnectionFactory.createConnection(config);
		searcher = new Searcher(conn);
		playerStats = new PlayerStatsFinder(conn);
		teamStats = new TeamStatsFinder(conn);
	}

	public void exit() {
		System.out.println("Exiting");
		try {
			conn.getAdmin().getConnection().close();
			System.exit(0);
		} catch (IOException e) {
			System.out.println("Error attempting to close connection");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void search(String line) {
		try {
			searcher.search(line.contains("-n"), line);
		} catch (MismatchedArgsException e) {
			System.out.println(e.getMessage());
		}
	}

	private void searchPlayerStats(String line) {
		try {
			playerStats.search(line.contains("-n"), line);
		} catch (MismatchedArgsException e) {
			System.out.println(e.getMessage());
		}
	}

	private void searchTeamStats(String line) {
		try {
			teamStats.search(line.contains("-n"), line);
		} catch (MismatchedArgsException e) {
			System.out.println(e.getMessage());
		}
	}

	private void printTables() {
		try {
			TableName[] tables = getTables();
			System.out.println("Table Names: " + Arrays.toString(tables));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private TableName[] getTables() throws IOException {
		System.out.println("Retrieving Tables");
		return conn.getAdmin().listTableNames();
	}

	private void printHelp() {
		System.out.println("\n--------------HELP FOR PLAYBALL------------------");
		System.out.println("\n\nThe -year tag must be used if available\n\n");
		System.out.println(
				"Search [-homeTeam teamName] [-awayTeam teamName] [-year year] [-month month] [-day day] [-startTime hour] [-plays] "
						+ "\n  \"Used to search through games. Returns all games matching all provided fields.\"");
		System.out.println("\n\n");
		System.out.println("Play \n  \"Shows each play in a game once a single game has been found by using Search\"");
		System.out.println("\n\n");
		System.out.println(
				"PlayerStats [-year year] [-team teamName] [-firstName PlayerFirstName] [-lastName PlayerLastName]"
						+ " \n  \"Displays Player batting stats for the given year for players matching all of the provided fields\"");
		System.out.println("\n\n");
		System.out.println("TeamStats [-year year] [-team teamName] [-division division] [-city citName]"
				+ "\n  For Division please enter either A for American League or N for National League"
				+ "\n  \"Returns team stats for all teams in the given year whose team name contains the given team name and "
				+ "\n  matches the given division");
		System.out.println("\n\n");
		System.out.println("-------------------------------------------------");
	}

	public static void main(String[] args) {
		boolean debugMode = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-d")) {
				debugMode = true;
			}
		}
		initLogger();
		Main main = new Main(debugMode);
		main.run();
	}

	public static void initLogger() {
		PatternLayout layout = new PatternLayout("%-5p %d %m%n");
		FileAppender appender = new FileAppender();
		appender.setName("PlayBall");
		appender.setFile("playBall.log");
		appender.activateOptions();
		appender.setLayout(layout);
		Logger.getRootLogger().addAppender(appender);
	}
}
