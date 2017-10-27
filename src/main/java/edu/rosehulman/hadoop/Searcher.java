package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Searcher {

	private Connection conn;
	private String homeTeam;
	private String awayTeam;
	private String year;
	private String month;
	private String day;
	private String startTime;

	public Searcher(Connection connection) {
		resetFields();
		conn = connection;
	}

	public void search(boolean newSearch, String line) {
		if (newSearch) {
			resetFields();
		}
		try {
			search(line);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MismatchedArgsException("Wrong number of arguments for the specified fields");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Failed when looking up " + this);
		}
	}

	private void search(String line) throws IOException {
		String[] tokens = line.split("\\s+");
		parseFields(tokens);
		if (!isYearValid()) {
			System.out.println("Please search a valid year");
			return;
		}
		if (Main.debug) {
			System.out.println(this);
		}
		performSearch();
	}

	private void resetFields() {
		homeTeam = "";
		awayTeam = "";
		year = "";
		day = "";
		month = "";
		startTime = "";
	}

	private void parseFields(String[] tokens) {
		for (int i = 0; i < tokens.length; i++) {
			switch (tokens[i]) {
			case ("-homeTeam"):
				homeTeam = tokens[i + 1];
				break;
			case ("-awayTeam"):
				awayTeam = tokens[i + 1];
				break;
			case ("-year"):
				year = tokens[i + 1];
				break;
			case ("-day"):
				day = tokens[i + 1];
				break;
			case ("-month"):
				month = tokens[i + 1];
				break;
			case ("-startTime"):
				startTime = tokens[i + 1];
				break;
			default:
				break;
			}
		}
	}

	private boolean isYearValid() throws IOException {
		TableName[] tables = conn.getAdmin().listTableNames();
		for (int i = 0; i < tables.length; i++) {
			if (tables[i].toString().contains(year)) {
				return true;
			}
		}
		return false;
	}

	private void performSearch() throws IOException {
		String homeTeamID;
		String awayTeamID;
		if (!homeTeam.isEmpty() && !awayTeam.isEmpty()) {
			try {
				homeTeamID = getTeamID(homeTeam);
				awayTeamID = getTeamID(awayTeam);
				System.out.println("FOUND homeTeamID " + homeTeamID);
				System.out.println("FOUND awayTeamID " + awayTeamID);
				searchGamesWithBothTeamIDs(homeTeamID, awayTeamID);
			} catch (MismatchedArgsException e) {
				System.out.println(e.getMessage());
				return;
			}
		}
	}

	private String getTeamID(String teamName) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		ResultScanner scanner = table.getScanner(new Scan());
		Iterator<Result> results = scanner.iterator();
		Result result = null;
		String foundTeam = null;
		while (results.hasNext()) {
			result = results.next();
			foundTeam = Bytes.toString(result.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("name")));
			if (teamName.equals(foundTeam)) {
				return Bytes.toString(result.getRow());
			}
		}
		throw new MismatchedArgsException("Team: " + teamName + " not found");
	}

	private void searchGamesWithBothTeamIDs(String home, String away) throws IOException {
		Table table = conn.getTable(TableName.valueOf("games" + year));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		String foundHomeTeam = null;
		String foundAwayTeam = null;
		String foundMonth = null;
		String foundDay = null;
		String foundHour = null;
		List<Result> resultsFound = new ArrayList<Result>();
		System.out.println("Searching Games With Both Team IDs");
		for (Result result : scanner) {
			foundHomeTeam = Bytes.toString(result.getValue(Bytes.toBytes("team_data"), Bytes.toBytes("home_team")));
			foundAwayTeam = Bytes.toString(result.getValue(Bytes.toBytes("team_data"), Bytes.toBytes("away_team")));
			foundMonth = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("month")));
			foundDay = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("day")));
			foundHour = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("time")));
			if (resultMatches(foundHomeTeam, foundAwayTeam, foundMonth, foundDay, foundHour)) {
				System.out.println(result);
				resultsFound.add(result);
			}
		}
		if (resultsFound.size() > 1) {
			printGameResults(resultsFound);
		} else if (resultsFound.isEmpty()) {
			System.out.println("No Results Found");
		} else {
			// must be 1
		}

	}

	private boolean resultMatches(String foundHomeTeam, String foundAwayTeam, String foundMonth, String foundDay,
			String foundHour) {
		if (!homeTeam.isEmpty() && !homeTeam.equals(foundHomeTeam)) {
			return false;
		}
		if (!awayTeam.isEmpty() && !awayTeam.equals(foundAwayTeam)) {
			return false;
		}
		if (!day.isEmpty() && !day.equals(foundDay)) {
			return false;
		}
		if (!month.isEmpty() && !month.equals(foundMonth)) {
			return false;
		}
		if (compareHourToStartTime(foundHour)) {
			return false;
		}
		return true;
	}

	private boolean compareHourToStartTime(String time) {
		return !startTime.isEmpty() && time.startsWith(startTime.substring(0, 1));
	}

	private void printGameResults(List<Result> resultsFound) throws IOException {
		for (Result result : resultsFound) {
			printGameResult(result);
		}
	}

	private void printGameResult(Result result) throws IOException {
		String foundHomeTeam = Bytes.toString(result.getValue(Bytes.toBytes("team_data"), Bytes.toBytes("home_team")));
		String foundAwayTeam = Bytes.toString(result.getValue(Bytes.toBytes("team_data"), Bytes.toBytes("away_team")));
		String foundMonth = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("month")));
		String foundDay = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("day")));
		String foundHour = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("time")));
		StringBuilder builder = new StringBuilder();
		builder.append("Game between ");
		builder.append(getTeamName(foundHomeTeam) + " and ");
		builder.append(getTeamName(foundAwayTeam) + " ");
		builder.append("at " + foundHour + " ");
		builder.append("on " + foundDay + "/" + foundMonth + "/" + year);
		System.out.println(builder.toString());
	}

	private String getTeamName(String teamCode) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		Get get = new Get(Bytes.toBytes(teamCode));
		Result res = table.get(get);
		return Bytes.toString(res.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("name")));
	}

	@Override
	public String toString() {
		return "Searcher [homeTeam=" + homeTeam + ", awayTeam=" + awayTeam + ", year=" + year + ", month=" + month
				+ ", day=" + day + ", startTime=" + startTime + "]";
	}

}
