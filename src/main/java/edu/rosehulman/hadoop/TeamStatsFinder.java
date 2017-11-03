package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class TeamStatsFinder {
	private Connection conn;
	private String cityName;
	private String division;
	private String teamName;
	private String year;

	public TeamStatsFinder(Connection connection) {
		resetFields();
		conn = connection;
	}

	private void resetFields() {
		cityName = "";
		division = "";
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
			case ("-team"):
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
			case ("-city"):
				cityName = tokens[i + 1];
				if (i + 2 < tokens.length) {
					if (!tokens[i + 2].contains("-")) {
						cityName += " " + tokens[i + 2];
					}
				}
				break;
			case ("-division"):
				division = tokens[i + 1];
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
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
		Scan scan = new Scan();
		boolean needFilter = false;
		if (!cityName.isEmpty()) {
			SingleColumnValueFilter firstnameFilter = new SingleColumnValueFilter(Bytes.toBytes("teams_data"),
					Bytes.toBytes("city"), CompareOp.EQUAL, new SubstringComparator(cityName));
			filter.addFilter(firstnameFilter);
			needFilter = true;
		}
		if (!division.isEmpty()) {
			SingleColumnValueFilter lastnameFilter = new SingleColumnValueFilter(Bytes.toBytes("teams_data"),
					Bytes.toBytes("division"), CompareOp.EQUAL, new SubstringComparator(division));
			filter.addFilter(lastnameFilter);
			needFilter = true;
		}
		if (!teamName.isEmpty()) {
			SingleColumnValueFilter teamFilter = new SingleColumnValueFilter(Bytes.toBytes("teams_data"),
					Bytes.toBytes("name"), CompareOp.EQUAL, new SubstringComparator(teamName));
			filter.addFilter(teamFilter);
			needFilter = true;
		}
		if (needFilter) {
			scan.setFilter(filter);
		}

		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iter = scanner.iterator();
		if (iter.hasNext()) {
			printTeamStats(iter);
		} else {
			System.out.println("No Results Found");
		}
	}

	private void printTeamStats(Iterator<Result> iter) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teamstats" + year));
		Get get = null;
		Result statsResult = null;
		Result res = null;
		String actualThingIHaveToLookUp = "";
		while (iter.hasNext()) {
			res = iter.next();
			actualThingIHaveToLookUp = "\"" + Bytes.toString(res.getRow()) + "\"";
			get = new Get(Bytes.toBytes(actualThingIHaveToLookUp));
			statsResult = table.get(get);
			System.out.println(statsResult.getRow());
			if (!statsResult.isEmpty()) {
				printTeamStats(statsResult);
			}
		}
	}

	private void printTeamStats(Result res) throws IOException {
		String[] teamNameAndDivision = getTeamNameAndDivision(Bytes.toString(res.getRow()));
		String name = teamNameAndDivision[0];
		String division = teamNameAndDivision[1];

		String runs = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("runs")));
		String gamesPlayed = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("games")));
		String wins = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("wins")));
		String loses = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("loses")));
		String avgTime = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("avgTime")));
		String avgAttendance = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("avgAttendance")));

		System.out.println("Team: " + name + "in the " + translateDivision(division) + "\n\tPlayed " + gamesPlayed
				+ " games" + "\n\twith an average game time of " + (int) Double.parseDouble(avgTime)
				+ " minutes\n\tand an average attendance of " + (int) Double.parseDouble(avgAttendance) + " people"
				+ "\n\tWins: " + wins + "\n\tloses: " + loses + "\n\truns earned: " + runs);
	}

	private String[] getTeamNameAndDivision(String teamCode) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		Get get = new Get(Bytes.toBytes(teamCode));
		Result res = table.get(get);
		String[] output = new String[2];
		output[0] = Bytes.toString(res.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("name")));
		output[1] = Bytes.toString(res.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("division")));
		return output;
	}

	private String translateDivision(String divCode) {
		if (divCode == null) {
			return "Info Not Present";
		}
		if (divCode.equals("A")) {
			return "American League";
		}
		return "National League";
	}

	public String toString() {
		return "TeamStatsFinder [cityName=" + cityName + ", division=" + division + ", teamName=" + teamName + ", year="
				+ year + "]";
	}
}
