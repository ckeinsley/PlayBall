package edu.rosehulman.hadoop;

import org.apache.hadoop.hbase.client.Admin;

public class Searcher {

	private Admin admin;
	private String homeTeam;
	private String awayTeam;
	private String year;
	private String month;
	private String day;
	private String startTime;

	public Searcher(Admin hbaseAdmin) {
		resetFields();
		admin = hbaseAdmin;
	}

	public void search(boolean newSearch, String line) {
		if (newSearch) {
			resetFields();
		}
		String[] tokens = line.split("\\s+");
		try {
			parseFields(tokens);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MismatchedArgsException("Wrong number of arguments for the specified fields");
		}
	}

	private void resetFields() {
		// change fields back to "";
	}

	private void parseFields(String[] tokens) {
		for (int i = 0; i < tokens.length; i++) {

		}
	}

}
