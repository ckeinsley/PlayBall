package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PlayerStatsFinder {

	private Connection conn;
	private String fName;
	private String lName;
	private String teamName;
	private String year;

	public PlayerStatsFinder(Connection connection) {
		resetFields();
		conn = connection;
	}

	private void resetFields() {
		fName = "";
		lName = "";
		teamName = "";
		year = "";
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
			case ("-teamName"):
				teamName = tokens[i + 1];
				if (i + 2 < tokens.length) {
					if (!tokens[i + 2].contains("-")) {
						teamName += " " + tokens[i + 2];
					}
				}
				break;
			case ("-year"):
				year = tokens[i + 1];
				break;
			case ("-firstName"):
				fName = tokens[i + 1];
				break;
			case ("-lastName"):
				lName = tokens[i + 1];
				break;
			default:
				break;
			}
		}
	}

	private boolean isYearValid() throws IOException {
		if (year.isEmpty()) {
			return false;
		}
		TableName[] tables = conn.getAdmin().listTableNames();
		for (int i = 0; i < tables.length; i++) {
			if (tables[i].toString().contains(year)) {
				return true;
			}
		}
		return false;
	}

	private void performSearch() throws IOException {
		Table table = conn.getTable(TableName.valueOf("players" + year));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		String foundFirstName = null;
		String foundLastName = null;
		String foundTeamID = null;
		List<Result> resultsFound = new ArrayList<Result>();
		for (Result result : scanner) {
			foundFirstName = Bytes.toString(result.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("firstname")));
			foundLastName = Bytes.toString(result.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("lastname")));
			foundTeamID = Bytes.toString(result.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("teamId")));
			if (resultMatches(foundFirstName, foundLastName, foundTeamID)) {
				resultsFound.add(result);
			}
		}
		if (resultsFound.size() != 0) {
			printPlayerStats(resultsFound);
		} else if (resultsFound.isEmpty()) {
			System.out.println("No Results Found");
		}
	}

	private boolean resultMatches(String foundFirstName, String foundLastName, String foundTeamID) throws IOException {
		if (!fName.isEmpty() && !foundFirstName.equals(fName)) {
			return false;
		}
		if (!lName.isEmpty() && !foundLastName.equals(lName)) {
			return false;
		}
		if (!teamName.isEmpty() && !teamName.equals(getTeamName(foundTeamID))) {
			return false;
		}
		return true;
	}

	private String getTeamName(String teamCode) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		Get get = new Get(Bytes.toBytes(teamCode));
		Result res = table.get(get);
		return Bytes.toString(res.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("name")));
	}

	private void printPlayerStats(List<Result> playersFoundResults) throws IOException {
		Table table = conn.getTable(TableName.valueOf("playersStats" + year));
		Get get = null;
		Result statsResult = null;
		for (Result res : playersFoundResults) {
			get = new Get(res.getRow());
			statsResult = table.get(get);
			if (!statsResult.isEmpty()) {
				printPlayerStats(statsResult);
			}
		}
	}

	private void printPlayerStats(Result res) throws IOException {
		String name = lookupPlayer(Bytes.toString(res.getRow()));
		String atBats = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("AB")));
		String average = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("avg")));
		String hits = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hits")));
		String singles = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("singles")));
		String doubles = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("doubles")));
		String triples = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("triples")));
		String homeRuns = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hrs")));
		System.out.println("Player: " + name + "\n\tAt Bats: " + atBats + "\n\tBatting Average: " + average
				+ "\n\tHits: " + hits + "\n\tSingles: " + singles + "\n\tDoubles: " + doubles + "\n\tTriples: "
				+ triples + "\n\tHome Runs: " + homeRuns);
	}

	private String lookupPlayer(String foundPlayerId) throws IOException {
		System.out.println("Player found : " + foundPlayerId);
		Table table = conn.getTable(TableName.valueOf("players" + year));
		Get get = new Get(Bytes.toBytes(foundPlayerId));
		Result res = table.get(get);
		return Bytes.toString(res.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("firstname"))) + " "
				+ Bytes.toString(res.getValue(Bytes.toBytes("players_data"), Bytes.toBytes("lastname")));
	}

	@Override
	public String toString() {
		return "PlayerStatsFinder [conn=" + conn + ", fName=" + fName + ", lName=" + lName + ", teamName=" + teamName
				+ ", year=" + year + "]";
	}
}
