package edu.indiana.d2i.zong.ingest.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.io.Files;

import edu.indiana.d2i.zong.ingest.version.Constants;
import edu.indiana.d2i.zong.ingest.version.cassandra.CassandraManager;

public class ExperimentSetConstructor {

	public static void main(String[] args) throws InterruptedException {
		/**
		 * args[0] is the input volume id list file path
		 * args[1] is the number of threads to get the volume zip and mets file
		 * args[2] is the source of data. there are only 2 valid value: cassandra or pairtree
 		 */
		System.out.println("loading volume ids...");
        List<String> volumeIds = Tools.getVolumeIds(new File(args[0]));
        System.out.println("loaded volume ids...");
		int threadCount = Integer.valueOf(args[1]);
		ExecutorService executorService = Executors.newFixedThreadPool(20);
		//int splitSize = volumeIds.size() / threadCount;
		List<String>[] volumeIdSplits = new LinkedList[threadCount];
		System.out.println("splitting volume ids...");
		for(int i=0; i<volumeIdSplits.length; i++) {
			volumeIdSplits[i] = new LinkedList<String>();
		}
		//System.out.println("splitted volume ids...");
		for(int i=0; i<volumeIds.size(); i++) {
			volumeIdSplits[i%threadCount].add(volumeIds.get(i));
		}
		System.out.println("splitted volume ids...");
		if(args[2].equals("cassandra")) {
			System.out.println("fetching data form cassandra...");
			CassandraManager cassandraManager = CassandraManager.getInstance();
			for(int i=0; i<threadCount; i++) {
				ZipMetsCassandraFetcher fetcher = new ZipMetsCassandraFetcher(volumeIdSplits[i], cassandraManager);
				System.out.println("run fetcher " + i);
				executorService.execute(fetcher);
			}
			executorService.shutdown();
			executorService.awaitTermination(2, TimeUnit.DAYS);
			cassandraManager.shutdown();
			System.out.println("done with fetching data form cassandra...");
		} else if(args[2].equals("pairtree")) {
			for(int i=0; i<threadCount; i++) {
				ZipMetsPairtreeFetcher fetcher = new ZipMetsPairtreeFetcher(volumeIdSplits[i]);
				System.out.println("run fetcher " + i);
				executorService.execute(fetcher);
			}
			executorService.shutdown();
			executorService.awaitTermination(2, TimeUnit.DAYS);
		}
		
	}

	public static class ZipMetsPairtreeFetcher implements Runnable {
		private List<String> idList;
		private String sourcePath;
		private String destPath;
		public ZipMetsPairtreeFetcher(List<String> idList) {
			this.idList = idList;
			this.sourcePath = Configuration.getProperty("ROOT_PATH");
			this.destPath = Configuration.getProperty("VOLUME_DIR");
		}
		
		@Override
		public void run() {
			for(String volumeId: idList) {
				try {
					copy(sourcePath, destPath, volumeId);
					System.out.println("fetched volume: " + volumeId);
				} catch (IOException e) {
					System.out.println("ioexception for " + volumeId);
					System.out.println(e.getMessage());
				}
			}
		}

		private void copy(String sourcePath, String destPath, String volumeId) throws IOException {
			String fromPath = Tools.getPairtreePath(volumeId);
			StringBuilder absoluteFromPathBuilder = new StringBuilder();
			String fromAbsolutePath = absoluteFromPathBuilder.append(Configuration.getProperty("ROOT_PATH")).append(Constants.SEPERATOR)
			.append(fromPath).toString();
			
			String cleanId = Tools.cleanId(volumeId);
			String dirName = cleanId.split("\\.", 2)[1];
			File sourceZip = new File(fromAbsolutePath, dirName+".zip");
			File metsFile = new File(fromAbsolutePath, dirName + ".mets.xml");
			File destDir = new File(destPath, dirName); 
			destDir.mkdirs();
			File destZip = new File(destDir, dirName+".zip");
			File destMets = new File(destDir, dirName + ".mets.xml");
			
			Files.copy(sourceZip, destZip);
			Files.copy(metsFile, destMets);
		}
		
	}
	
	public static class ZipMetsCassandraFetcher implements Runnable {
		private List<String> idList;
		private CassandraManager cassandraManager;
		public ZipMetsCassandraFetcher(List<String> idList, CassandraManager cassandraManager) {
			this.idList = idList;
			this.cassandraManager = cassandraManager;
		}
		
		@Override
		public void run() {
			for(String volumeId : idList) {
				try {
					fetch(volumeId);
				} catch (IOException e) {
					System.out.println(volumeId + ": " + e.getMessage());
				}
			}
			
		}

		private void fetch(String volumeId) throws IOException {
			Statement select = QueryBuilder.select()/*.column("volumeid")*/.column("volumezip").column("structmetadata")
					.from(cassandraManager.getKeySpace(), Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY))
					.where(QueryBuilder.eq("volumeid", volumeId))
					.limit(1);
			ResultSet res = cassandraManager.execute(select);
			List<Row> rows = res.all();
			if(rows.isEmpty()) {
				System.out.println("empty result for " + volumeId);
				return;
			} else if(rows.size() > 1) {
				System.out.println("!! more than one volume result for " + volumeId);
			} else {
				Row row = rows.get(0);
				ByteBuffer volumeZip = row.getBytes("volumezip");
				String mets = row.getString("structmetadata");
				writeVolume(volumeId, volumeZip, mets, Configuration.getProperty("VOLUME_DIR"));
				System.out.println("fetched volume: " + volumeId );
			}
		}

		private void writeVolume(String volumeId, ByteBuffer volumeZip, String mets, String parentDirPath) throws IOException {
			String cleanId = Tools.cleanId(volumeId);
			String dirName = cleanId.split("\\.", 2)[1];
			File dir = new File(parentDirPath, dirName);
			dir.mkdirs();
			File volumeZipFile = new File(dir, dirName+".zip");
			File metsFile = new File(dir, dirName + ".mets.xml");
			FileChannel volumeZipFileChannel = new FileOutputStream(volumeZipFile, false).getChannel();
			volumeZipFileChannel.write(volumeZip);
			volumeZipFileChannel.close();
			
			FileChannel metsFileChannel =  new FileOutputStream(metsFile, false).getChannel();
			metsFileChannel.write(ByteBuffer.wrap(mets.getBytes()));
			metsFileChannel.close();
			
		}
		
	}
}
