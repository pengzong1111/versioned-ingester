package edu.indiana.d2i.zong.ingest.version;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.zong.ingest.util.Configuration;
import edu.indiana.d2i.zong.ingest.util.Tools;
import edu.indiana.d2i.zong.ingest.version.cassandra.CassandraVersionedIngester;

public class VersionedIngestService {
	private static Logger log = LogManager.getLogger(VersionedIngestService.class);
	public static void main(String[] args) {
		log.info("load volume ids to ingest...");
		List<String> volumesToIngest = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		
		VersionedIngester cassandraIngester = new CassandraVersionedIngester();
		cassandraIngester.ingest(volumesToIngest);
		cassandraIngester.close();
		/*VersionedIngester solrIngester = new SolrVersionedIngester();
		solrIngester.ingest(successIds);
				
		VersionedIngester redisIngester = new RedisVersionedIngester();
		redisIngester.ingest(successIds);*/
	}
}
