package edu.indiana.d2i.zong.ingest.version;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

public abstract class VersionedIngester {
	public VersionedIngester() {
		try {
			pw = new PrintWriter("success.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	private PrintWriter pw ;
	/**
	 * get a list of volume ids to ingest
	 * @param idList	a list of id to ingest
	 */
	public void ingest(List<String> idList) {
		for(String id : idList) {
			if(ingestOne(id)) {
				pw.println(id);
				pw.flush();
			}
		}
		pw.close();
	}
	
	public abstract boolean ingestOne(String volumeId);
	
	/**
	 * release all related/used resources
	 */
	public abstract void close();
}
