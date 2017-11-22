package edu.indiana.d2i.zong.ingest.access;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import difflib.PatchFailedException;
import edu.indiana.d2i.zong.ingest.util.Configuration;
import edu.indiana.d2i.zong.ingest.util.MyersDiffTool;
import edu.indiana.d2i.zong.ingest.version.Constants;
import edu.indiana.d2i.zong.ingest.version.cassandra.CassandraManager;

public class VersionedDataFetcher {
	Logger log = LogManager.getLogger(VersionedDataFetcher.class);
	CassandraManager cassandraManager;
	
	public VersionedDataFetcher() {
		this.cassandraManager = CassandraManager.getInstance();
	}
	
	/**
	 * get contents of volume that has version number less than or equal to version and is not rmoved
	 * @param volumeId
	 * @param version
	 * @return a map from page sequence to page content
	 */
	public Map<String, String> getLatestData(String volumeId, int version) {
		System.out.println("fetching data...");
		LatestVersionStatusInfo latestVersionStatusInfo = getLatestVersionAndStatus(volumeId, version);
		if(latestVersionStatusInfo == null) return null;
		if(latestVersionStatusInfo.isBase()) {
			Map<String, String> seqToContentMap= getPages(volumeId, version);
			return seqToContentMap;
		} else {
			int baseVersion = latestVersionStatusInfo.getBaseVersion();
			int latestVersion = latestVersionStatusInfo.getLatestVersion();
			System.out.println("base version: " + baseVersion);
			System.out.println("latest version: " + latestVersion);
			Map<String, String> seqToContentMap= getPages(volumeId, baseVersion);
			Map<String, String> seqToDeltaMap= getPages(volumeId, latestVersion);
			Map<String, String> resultMap = new HashMap<String, String>();
			for(String sequence : seqToContentMap.keySet()) {
				String diff = seqToDeltaMap.get(sequence);
				String baseText = seqToContentMap.get(sequence);
				try {
			//		System.out.println("base: " + baseText);
			//		System.out.println("diff: " + diff);
					String latestText = MyersDiffTool.getRevisedString(baseText, diff);
					resultMap.put(sequence, latestText);
				} catch (PatchFailedException e) {
					log.error("patch failed for " + volumeId + " version: " + version, e.getMessage());
					System.out.println("patch failed for " + volumeId + " version: " + version);
					e.printStackTrace();
				}
			}
			return resultMap;
		}
		
	}
	
	
	private Map<String, String> getPages(String volumeId, int version) {
		Statement select = QueryBuilder.select().column("contents").column("volumeid").column("sequence").column("version").column("pageNumberLabel")
				.from(cassandraManager.getKeySpace(), Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY)).where(QueryBuilder.eq("volumeid", volumeId))
				.and(QueryBuilder.eq("version", version));
				//.and(QueryBuilder.eq("sequence", sequence))
				//.and(QueryBuilder.eq("pageNumberLabel", pageNumberLabel));
		System.out.println("getting pages with query: " + select);
		ResultSet result = cassandraManager.execute(select);
		Map<String, String> sequenceToContentMap = new HashMap<String, String>();
		List<Row> rows = result.all();
		if(rows.size() == 0) {
			log.error("no version "+ version +" for " + volumeId);
			System.out.println("no version "+ version +" for " + volumeId);
			return null;
		} else {
			System.out.println(rows.size());
			for(Row row : rows) {
				String sequence = row.getString("sequence");
				String text = row.getString("contents");
			//	String label = row.getString("pageNumberLabel");
				sequenceToContentMap.put(sequence, text);
			}
			return sequenceToContentMap;
			/*System.out.println(rows.get(0).getString("pageNumberLabel"));
			if(!rows.get(0).getString("pageNumberLabel").equals(pageNumberLabel)) {
				pwMismatchedSequence.println(volumeId); pwMismatchedSequence.flush();
			}
			return text;*/
		}
	
	}

	private LatestVersionStatusInfo getLatestVersionAndStatus(String volumeId, int version) {
		// select volumeid, version from table where volumeid=volumeId AND version <= version limit 1;\
		Statement select = QueryBuilder.select()/*.column("volumeid")*/.column("version").column("isRemoved").column("isBase").column("baseVersion")
				.from(cassandraManager.getKeySpace(), Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY))
				.where(QueryBuilder.eq("volumeid", volumeId))
				.and(QueryBuilder.lte("version", version))
				.orderBy(QueryBuilder.desc("version"))
				.limit(1);
		System.out.println("issuing query: " + select.toString());
		ResultSet result = cassandraManager.execute(select);
		List<Row> rows = result.all();
		System.out.println("got #pages: " + rows.size());
		if(rows.size() == 0) {
			log.error("no version <= " + version + " for " + volumeId);
			return null;
		} else {
			LatestVersionStatusInfo info = new LatestVersionStatusInfo();
			info.setVolumeId(volumeId);
			Row row = rows.get(0);
			if(row.isNull("version")) {
				log.error("latest version is null for " + volumeId + " version " + version);
			} else {
				info.setLatestVersion(row.getInt("version"));
			}
			
			if(row.isNull("baseVersion")) {
				log.error("baseVersion is null for " + volumeId);
			} else {
				info.setBaseVersion(row.getInt("baseVersion"));
			}
			
			if(row.isNull("isRemoved")) {
				log.error("isRemoved is null for " + volumeId + " version " + version);
			} else {
				info.setRemoved(row.getBool("isRemoved"));
			}
			if(row.isNull("isBase")) {
				log.error("isBase is null for " + volumeId + " version " + version);
			} else {
				System.out.println("isbase: " + row.getBool("isBase"));
				info.setBase(row.getBool("isBase"));
			}
			return info;
		}
	}

	public void close() {
		cassandraManager.shutdown();
	}
	public static void main(String[] args) {
		String volumeId = args[0];
		int version = Integer.valueOf(args[1]);
		VersionedDataFetcher fetcher = new VersionedDataFetcher();
		Map<String, String> result = fetcher.getLatestData(volumeId, version);
		for(Map.Entry<String, String> entry : result.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println("---------------");
			System.out.println(entry.getValue());
		}
		fetcher.close();
	}

	static class LatestVersionStatusInfo {
		private int latestVersion;
		private boolean isRemoved;
		private boolean isBase;
		private String volumeId;
		private int baseVersion;
		public int getLatestVersion() {
			return latestVersion;
		}
		public void setBaseVersion(int baseVersion) {
			this.baseVersion = baseVersion;
		}
		public int getBaseVersion() {
			return baseVersion;
		}
		public void setLatestVersion(int latestVersion) {
			this.latestVersion = latestVersion;
		}
		public boolean isRemoved() {
			return isRemoved;
		}
		public void setRemoved(boolean isRemoved) {
			this.isRemoved = isRemoved;
		}
		public boolean isBase() {
			return isBase;
		}
		public void setBase(boolean isBase) {
			this.isBase = isBase;
		}
		public String getVolumeId() {
			return volumeId;
		}
		public void setVolumeId(String volumeId) {
			this.volumeId = volumeId;
		}
	}
}
