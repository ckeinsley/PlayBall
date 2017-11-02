package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private Map<String, String> pitchLookup;
	private String homeTeam;
	private String awayTeam;
	private String year;
	private String month;
	private String day;
	private String startTime;
	private String foundGameId;
	private int playIndex;

	public Searcher(Connection connection) {
		resetFields();
		conn = connection;
		pitchLookup = new HashMap<>();
		setupPitchLookupMap();
	}

	private void resetFields() {
		homeTeam = "";
		awayTeam = "";
		year = "";
		day = "";
		month = "";
		startTime = "";
		foundGameId = "";
		playIndex = 1;
	}

	private void setupPitchLookupMap() {
		pitchLookup.put("B", "Ball");
		pitchLookup.put("C", "Called Strike");
		pitchLookup.put("F", "Foul");
		pitchLookup.put("H", "Hit Batter");
		pitchLookup.put("I", "Intentional Ball");
		pitchLookup.put("K", "Strike (Unknown Type)");
		pitchLookup.put("L", "Foul Bunt");
		pitchLookup.put("M", "Missed Bunt Attempt");
		pitchLookup.put("N", "No Pitch");
		pitchLookup.put("O", "Foul Tip on Bunt");
		pitchLookup.put("P", "Pitchout");
		pitchLookup.put("Q", "Swinging on pitchout");
		pitchLookup.put("R", "Foul ball on pitchout");
		pitchLookup.put("S", "Swinging on pitchout");
		pitchLookup.put("T", "Swinging Strike");
		pitchLookup.put("T", "Foul Tip");
		pitchLookup.put("U", "Unknown or missed pitch");
		pitchLookup.put("V", "Called ball, because pitcher went to his mouth");
		pitchLookup.put("X", "Ball put into play by batter");
		pitchLookup.put("Y", "Ball put into play on pitchout");

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

	private void parseFields(String[] tokens) {
		for (int i = 0; i < tokens.length; i++) {
			switch (tokens[i]) {
			case ("-homeTeam"):
				homeTeam = tokens[i + 1];
				if (i + 2 < tokens.length) {
					if (!tokens[i + 2].contains("-")) {
						homeTeam += " " + tokens[i + 2];
					}
				}
				break;
			case ("-awayTeam"):
				awayTeam = tokens[i + 1];
				if (i + 2 < tokens.length) {
					if (!tokens[i + 2].contains("-")) {
						awayTeam += " " + tokens[i + 2];
					}
				}
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
		searchGamesWithBothTeamIDs();
	}

	private void searchGamesWithBothTeamIDs() throws IOException {
		Table table = conn.getTable(TableName.valueOf("games" + year));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		String foundHomeTeam = null;
		String foundAwayTeam = null;
		String foundMonth = null;
		String foundDay = null;
		String foundHour = null;
		List<Result> resultsFound = new ArrayList<Result>();
		for (Result result : scanner) {
			foundHomeTeam = Bytes.toString(result.getValue(Bytes.toBytes("team_data"), Bytes.toBytes("home_team")));
			foundAwayTeam = Bytes.toString(result.getValue(Bytes.toBytes("team_data"), Bytes.toBytes("away_team")));
			foundMonth = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("month")));
			foundDay = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("day")));
			foundHour = Bytes.toString(result.getValue(Bytes.toBytes("date_time"), Bytes.toBytes("time")));
			if (resultMatches(foundHomeTeam, foundAwayTeam, foundMonth, foundDay, foundHour)) {
				resultsFound.add(result);
			}
		}
		if (resultsFound.size() > 1) {
			printGameResults(resultsFound);
		} else if (resultsFound.isEmpty()) {
			System.out.println("No Results Found");
		} else {
			System.out.println("Found exactly 1 game");
			printGameResults(resultsFound);
			foundGameId = Bytes.toString(resultsFound.get(0).getRow());
		}
	}

	private boolean resultMatches(String foundHomeTeam, String foundAwayTeam, String foundMonth, String foundDay,
			String foundHour) throws IOException {
		if (!homeTeam.isEmpty() && !homeTeam.equals(getTeamName(foundHomeTeam))) {
			return false;
		}
		if (!awayTeam.isEmpty() && !awayTeam.equals(getTeamName(foundAwayTeam))) {
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

	public void displayNextPlay() {
		try {
			if (foundGameId.isEmpty()) {
				System.out.println("Please narrow search to one game");
			}
			printPlayResult(getNextPlay(), playIndex);
			playIndex++;
		} catch (IOException e) {
			System.out.println("Failed to find play");
		}
	}

	private Result getNextPlay() throws IOException {
		System.out.println("Play id : " + playIndex + foundGameId);
		Table table = conn.getTable(TableName.valueOf("plays" + year));
		Get get = new Get(Bytes.toBytes(playIndex + foundGameId));
		Result res = table.get(get);
		return res;
	}

	private void printPlayResult(Result res, int playNum) throws IOException {
		String foundInning = Bytes.toString(res.getValue(Bytes.toBytes("play_data"), Bytes.toBytes("inning")));
		String foundPlayerId = Bytes.toString(res.getValue(Bytes.toBytes("play_data"), Bytes.toBytes("playerId")));
		String foundBatterCount = Bytes
				.toString(res.getValue(Bytes.toBytes("play_data"), Bytes.toBytes("batterCount")));
		String foundPitches = Bytes.toString(res.getValue(Bytes.toBytes("play_data"), Bytes.toBytes("pitches")));
		String foundTeam = Bytes.toString(res.getValue(Bytes.toBytes("play_data"), Bytes.toBytes("team")));

		System.out.println("Play Number " + playNum + " during Inning " + foundInning + ". Batter up: "
				+ lookupPlayer(foundPlayerId) + " for the " + getTeamName(foundTeam) + "\nBatter Count: "
				+ foundBatterCount + "\nPitches: " + translatePitches(foundPitches));
	}

	private String lookupPlayer(String foundPlayerId) throws IOException {
		Table table = conn.getTable(TableName.valueOf("players" + year));
		Get get = new Get(Bytes.toBytes(foundPlayerId));
		Result res = table.get(get);
		return Bytes.toString(res.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("firstname"))) + " "
				+ Bytes.toString(res.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("lastname")));
	}

	private String translatePitches(String pitches) {
		String[] tokens = pitches.split("(?!^)");
		StringBuilder builder = new StringBuilder();
		for (String token : tokens) {
			builder.append("\n\t");
			builder.append(pitchLookup.get(token));
		}
		builder.append("\n");
		return builder.toString();
	}

	@Override
	public String toString() {
		return "Searcher [homeTeam=" + homeTeam + ", awayTeam=" + awayTeam + ", year=" + year + ", month=" + month
				+ ", day=" + day + ", startTime=" + startTime + "]";
	}

}
