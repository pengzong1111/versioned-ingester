package edu.indiana.d2i.zong.analysis.page;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.zong.ingest.util.Configuration;
import edu.indiana.d2i.zong.ingest.util.MyersDiffTool;
import edu.indiana.d2i.zong.ingest.util.Tools;
import edu.indiana.d2i.zong.ingest.util.METSParser.VolumeRecord;
import edu.indiana.d2i.zong.ingest.util.METSParser.VolumeRecord.PageRecord;
import edu.indiana.d2i.zong.ingest.version.Constants;
import edu.indiana.d2i.zong.ingest.version.cassandra.CassandraManager;

public class PageDiffStats {
	Logger log = LogManager.getLogger(PageDiffStats.class);
	
	private PrintWriter pwStats;
	private PrintWriter pwNum;
	public PageDiffStats() {
		try {
			pwStats = new PrintWriter("page-diff-stats.txt");
			pwNum = new PrintWriter("page-changes-per-vol.txt");
		} catch (FileNotFoundException e) {
			log.error("error creating printwriter for checksum verification", e.getMessage());
		}
	}

	public void close() {
		pwStats.close();
		pwNum.close();
		CassandraManager.shutdown();
	}
	public static void main(String[] args) throws IOException {
		PageDiffStats pageDiffStats = new PageDiffStats();
		List<String> ids = Tools.getVolumeIds(new File(args[0])); // args[0] is the updated id list file path
		for(String volumeId : ids) {
			pageDiffStats.recordPageStats(volumeId);
		}
		pageDiffStats.close();
	}
	
	private void recordPageStats(String volumeId) throws IOException {
		String cleanId = Tools.cleanId(volumeId);
		//String pairtreePath = Tools.getPairtreePath(cleanId);
		int count = 0;
		String cleanIdPart = cleanId.split("\\.", 2)[1];
		String zipFileName = cleanIdPart  + Constants.VOLUME_ZIP_SUFFIX; // e.g.: ark+=13960=t02z18p54.zip
		String metsFileName = cleanIdPart + Constants.METS_XML_SUFFIX; // e.g.: ark+=13960=t02z18p54.mets.xml
		/*
		 *  get the zip file and mets file for this volume id based on relative path(leafPath) and zipFileName or metsFileName
		 *  e.g.: /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.zip
		 *  /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.mets.xml
		 */
		File volumeZipFile1 = null;
		File volumeMetsFile1 = null;
		File volumeZipFile2 = null;
		File volumeMetsFile2 = null;
		if(Configuration.getProperty("SOURCE").equalsIgnoreCase("pairtree")) {
			//volumeZipFile = Tools.getFileFromPairtree(pairtreePath, zipFileName);
		//	volumeMetsFile = Tools.getFileFromPairtree(pairtreePath, metsFileName);
		} else {
			//String commonPath = Configuration.getProperty("VOLUME_DIR"); //"/home/zong/ht_text_sample/data0";
			String commonPath1 = "/N/dc2/projects/htrc/zongpeng/data/second-version"; //"/home/zong/ht_text_sample/data0";
			String commonPath2 = "/N/dc2/projects/htrc/zongpeng/data/version-20171130";
			File volumeDir1 = new File(commonPath1, cleanIdPart);
			File volumeDir2 = new File(commonPath2, cleanIdPart);
			if(volumeDir1.exists() && volumeDir1.exists()) {
				volumeZipFile1 = new File(volumeDir1, zipFileName);
				volumeMetsFile1 = new File(volumeDir1, metsFileName);
				
				volumeZipFile2 = new File(volumeDir2, zipFileName);
				volumeMetsFile2 = new File(volumeDir2, metsFileName);
			}
		}
		
		if(volumeZipFile1 == null || volumeMetsFile1 == null || !volumeZipFile1.exists() || !volumeMetsFile1.exists()) {
			log.error("zip file or mets file does not exist for " + volumeId);
			return;
		}
		
		VolumeRecord volumeRecord1 = Tools.getVolumeRecord(volumeId, volumeMetsFile1);
		VolumeRecord volumeRecord2 = Tools.getVolumeRecord(volumeId, volumeMetsFile2);
		
		ZipInputStream zis1 = new ZipInputStream(new FileInputStream(volumeZipFile1));
		ZipInputStream zis2 = new ZipInputStream(new FileInputStream(volumeZipFile2));
		
		Map<String, String> map1 = new HashMap<String,String>();
		Map<String, String> map2 = new HashMap<String,String>();
		
		extractSequenceToContentMap(zis1, volumeRecord1, map1, volumeZipFile1);
		extractSequenceToContentMap(zis2, volumeRecord2, map2, volumeZipFile2);
	
		System.out.println(volumeId + " same #entry: " + (map1.size() == map2.size()));
		for(String sequence: map1.keySet()) {
			String oldContent = map1.get(sequence);
			String newContent = map2.get(sequence);
			String diffText = MyersDiffTool.getDiffString(oldContent, newContent);
			if(!diffText.equals("")) {
				pwStats.println(volumeId + '\t' + sequence + '\t' + oldContent.getBytes().length + '\t' + newContent.getBytes().length + '\t' + diffText.getBytes().length);
				pwStats.flush();
				count ++;
			}
		}
		// print volumeid    #page    #pages changed
		pwNum.println(volumeId + '\t' + map1.size() + '\t' + count);
		pwNum.flush();
	}

	
	private void extractSequenceToContentMap(ZipInputStream zis, VolumeRecord volumeRecord, Map<String, String> map, File zipVolumeFile) throws IOException {
		ZipEntry zipEntry = null;
		while ((zipEntry = zis.getNextEntry()) != null) {
			String entryName = zipEntry.getName();
			String entryFilename = extractEntryFilename(entryName);
			PageRecord pageRecord = volumeRecord.getPageRecordByFilename(entryFilename);
			if (pageRecord == null) {
				log.error("No PageRecord found by " + entryFilename + " in volume zip "	+ zipVolumeFile.getAbsolutePath());
				continue;
			}
			if (entryFilename != null && !"".equals(entryFilename)) {
				// 1. read page contents in bytes
				byte[] pageContents = readPagecontentsFromInputStream(zis);
				if (pageContents == null) {
					log.error("failed reading page contents for " + entryName + " of " + volumeRecord.getVolumeID());
					continue;
				}
				
				String pageContentsString = new String(pageContents, "utf-8");

				int order = pageRecord.getOrder();
				String sequence = generateSequence(order);
				pageRecord.setSequence(sequence);
				
				map.put(sequence, pageContentsString);
				/*
				if(diffText!= null && !diffText.equals("")) {
					
				}*/
			}
		}
		
	}

	private byte[] readPagecontentsFromInputStream(ZipInputStream zis) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		int read = -1;
		byte[] buffer = new byte[32767];
		try {
			while((read = zis.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
		} catch (IOException e) {
			log.error("error reading zip stream" + e.getMessage());
		}
		try {
			bos.close();
		} catch (IOException e) {
			log.error("IOException while attempting to close ByteArrayOutputStream()" + e.getMessage());
		}
		return bos.toByteArray();
	}

	
	/**
     * Method to extract the filename from a ZipEntry name
     * @param entryName name of a ZipEntry
     * @return extracted filename
     */
    protected String extractEntryFilename(String entryName) {
        int lastIndex = entryName.lastIndexOf('/');
        return entryName.substring(lastIndex + 1);
    }
    
    static final int SEQUENCE_LENGTH = 8;
    
    /**
     * Method to generate a fixed-length zero-padded page sequence number
     * @param order the ordering of a page
     * @return a fixed-length zero-padded page sequence number based on the ordering
     */
    protected String generateSequence(int order) {
        String orderString = Integer.toString(order);
        StringBuilder sequenceBuilder = new StringBuilder();
        
        int digitCount = orderString.length();
        for (int i = digitCount; i < SEQUENCE_LENGTH; i++) {
            sequenceBuilder.append('0');
        }
        sequenceBuilder.append(orderString);
        return sequenceBuilder.toString();
    }
}
