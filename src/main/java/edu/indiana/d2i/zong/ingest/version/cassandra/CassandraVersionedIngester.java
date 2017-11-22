package edu.indiana.d2i.zong.ingest.version.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import edu.indiana.d2i.zong.ingest.exception.UnexpectedResultSizeException;
import edu.indiana.d2i.zong.ingest.util.Configuration;
import edu.indiana.d2i.zong.ingest.util.MyersDiffTool;
import edu.indiana.d2i.zong.ingest.util.METSParser.VolumeRecord;
import edu.indiana.d2i.zong.ingest.util.METSParser.VolumeRecord.PageRecord;
import edu.indiana.d2i.zong.ingest.util.Tools;
import edu.indiana.d2i.zong.ingest.version.Constants;
import edu.indiana.d2i.zong.ingest.version.VersionedIngester;

public class CassandraVersionedIngester extends VersionedIngester{
	Logger log = LogManager.getLogger(CassandraVersionedIngester.class);
	private PrintWriter pwChecksumInfo;
	private PrintWriter pwHasDiff;
	private PrintWriter pwMismatchedSequence;
	private PrintWriter pwLabelMismatch;
	private PrintWriter pwX;
	private PrintWriter pwStats;
	private CassandraManager cassandraManager;
	private String columnFamilyName;
	private int VERSION_NO;
	public CassandraVersionedIngester() {
		this.cassandraManager = CassandraManager.getInstance();
		this.columnFamilyName = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY);
		VERSION_NO = Integer.valueOf(Configuration.getProperty("LATEST_CASSANDRA_VERSION"));
		if(! cassandraManager.checkTableExist(this.columnFamilyName)) {
			String createTableStr = "CREATE TABLE " + this.columnFamilyName + " ("
		    		+ "volumeID text, "
				//	+ "accessLevel int STATIC, "
		    		+ "volumeByteCount bigint STATIC, "
					+ "volumeCharacterCount int STATIC, "
		    		+ "isRemoved boolean, " // indicate if this version is removed
					+ "version int, " // a integer
					+ "isBase boolean, " // if true, then the contents are not delta
					+ "baseVersion int STATIC, "
		    		+ "sequence text, "
		    		+ "byteCount bigint, "
		    		+ "characterCount int, "
		    		+ "contents text, "
		    		+ "checksum text, "
		    		+ "checksumType text, "
		    		+ "pageNumberLabel text, "
		    		+ "PRIMARY KEY (volumeID, version, sequence))";
			cassandraManager.execute(createTableStr);
		}
		try {
			pwChecksumInfo = new PrintWriter("failedChecksumVolIds.txt");
			pwHasDiff = new PrintWriter("hasDiff.txt");
			pwMismatchedSequence = new PrintWriter("mismatchedPageSeq.txt");
			pwLabelMismatch = new PrintWriter("misMatchedLabel.txt");
			pwX = new PrintWriter("baseVersionErrors.txt");
			pwStats = new PrintWriter("stats.txt");
		} catch (FileNotFoundException e) {
			log.error("error creating printwriter for checksum verification", e.getMessage());
		}
	}

	public void close() {
		pwChecksumInfo.close();
		pwHasDiff.close();
		pwLabelMismatch.close();
		pwMismatchedSequence.close();
		pwStats.close();
		cassandraManager.shutdown();
	}

	@Override
	public boolean ingestOne(String volumeId) {
		String cleanId = Tools.cleanId(volumeId);
		String pairtreePath = Tools.getPairtreePath(cleanId);
		
		String cleanIdPart = cleanId.split("\\.", 2)[1];
		String zipFileName = cleanIdPart  + Constants.VOLUME_ZIP_SUFFIX; // e.g.: ark+=13960=t02z18p54.zip
		String metsFileName = cleanIdPart + Constants.METS_XML_SUFFIX; // e.g.: ark+=13960=t02z18p54.mets.xml
		/*
		 *  get the zip file and mets file for this volume id based on relative path(leafPath) and zipFileName or metsFileName
		 *  e.g.: /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.zip
		 *  /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.mets.xml
		 */
		File volumeZipFile = null;
		File volumeMetsFile = null;
		if(Configuration.getProperty("SOURCE").equalsIgnoreCase("pairtree")) {
			volumeZipFile = Tools.getFileFromPairtree(pairtreePath, zipFileName);
			volumeMetsFile = Tools.getFileFromPairtree(pairtreePath, metsFileName);
		} else {
			String commonPath = Configuration.getProperty("VOLUME_DIR"); //"/home/zong/ht_text_sample/data0";
			File volumeDir = new File(commonPath, cleanIdPart);
			if(volumeDir.exists()) {
				volumeZipFile = new File(volumeDir, zipFileName);
				volumeMetsFile = new File(volumeDir, metsFileName);
			}
		}
		
		if(volumeZipFile == null || volumeMetsFile == null || !volumeZipFile.exists() || !volumeMetsFile.exists()) {
			log.error("zip file or mets file does not exist for " + volumeId);
			return false;
		}
		
		VolumeRecord volumeRecord = Tools.getVolumeRecord(volumeId, volumeMetsFile);
		boolean volumeAdded = false;
		try {
			volumeAdded = updatePages(volumeZipFile, volumeRecord);
		} /*catch (FileNotFoundException e) {
			System.out.println("file not found " + e.getMessage());
			log.error("file not found" + e.getMessage());
		}*/ catch (UnexpectedResultSizeException e) {
			System.out.println("Unexpected Result Size Exception" + e.getMessage());
			log.error("Unexpected Result Size Exception" + e.getMessage());
		}
		if(volumeAdded) {
			log.info("text ingested successfully " + volumeId);
		} else {
			log.error("text ingest failed " + volumeId);
		}
		return volumeAdded;
	}
	
	private boolean updatePages(File volumeZipFile, VolumeRecord volumeRecord) throws UnexpectedResultSizeException {
		String volumeId = volumeRecord.getVolumeID();
		boolean volumeAdded = false;
		
		int baseVersion = baseVersion(volumeId);
		Map<String, String[]> sequenceToContentLabelPairMap = null;
		if(baseVersion != -1){
			sequenceToContentLabelPairMap = getBasePages(volumeId, baseVersion);
			if(sequenceToContentLabelPairMap == null) {
				pwX.println(volumeId + " has base version " + baseVersion + ". but sequenceToContentLabelPairMap is null");pwX.flush();
				return false;
			}
		} 
	//	if (hasBase == -1) {
			BatchStatement batchStmt = new BatchStatement(); // a batch to insert all pages of this volume

			long volumeByteCount = 0;
			long volumeCharacterCount = 0;
			int i = 0;
			try {
				Insert firstPageInsert = null;
				ZipInputStream zis = new ZipInputStream(new FileInputStream(volumeZipFile));
				ZipEntry zipEntry = null;
				while ((zipEntry = zis.getNextEntry()) != null) {
					String entryName = zipEntry.getName();
					String entryFilename = extractEntryFilename(entryName);
					PageRecord pageRecord = volumeRecord.getPageRecordByFilename(entryFilename);
					if (pageRecord == null) {
						log.error("No PageRecord found by " + entryFilename + " in volume zip "	+ volumeZipFile.getAbsolutePath());
						continue;
					}
					if (entryFilename != null && !"".equals(entryFilename)) {
						// 1. read page contents in bytes
						byte[] pageContents = readPagecontentsFromInputStream(zis);
						if (pageContents == null) {
							log.error("failed reading page contents for " + entryName + " of " + volumeId);
							continue;
						}

						// 2. check against checksum of this page declared in METS
						String checksum = pageRecord.getChecksum();
						String checksumType = pageRecord.getChecksumType();
						try {
							String calculatedChecksum = Tools.calculateChecksum(pageContents, checksumType);
							if (!checksum.equals(calculatedChecksum)) {
								log.warn("Actual checksum and checksum from METS mismatch for entry "
										+ entryName
										+ " for volume: "
										+ volumeId
										+ ". Actual: "
										+ calculatedChecksum
										+ " from METS: "
										+ checksum);
								log.info("Recording actual checksum");
								pageRecord.setChecksum(calculatedChecksum, checksumType);
								pwChecksumInfo.println(volumeId);
								pwChecksumInfo.flush();
								return volumeAdded; // directly return false if mismatch happens
							} else {
								log.info("verified checksum for page " + entryFilename + " of " + volumeId);
							}
						} catch (NoSuchAlgorithmException e) {
							log.error("NoSuchAlgorithmException for checksum algorithm " + checksumType);
							log.error("Using checksum found in METS with a leap of faith");
						}

						// 3. verify byte count of this page
						if (pageContents.length != pageRecord.getByteCount()) {
							log.warn("Actual byte count and byte count from METS mismatch for entry "
									+ entryName
									+ " for volume "
									+ volumeId
									+ ". Actual: "
									+ pageContents.length
									+ " from METS: "
									+ pageRecord.getByteCount());
							log.info("Recording actual byte count");
							pageRecord.setByteCount(pageContents.length);
							volumeByteCount += pageContents.length;
						} else {
							volumeByteCount += pageRecord.getByteCount();
							log.info("verified page content for page "
									+ entryFilename + " of " + volumeId);
						}

						// 4. get 8-digit sequence for this page
						int order = pageRecord.getOrder();
						String sequence = generateSequence(order);
						pageRecord.setSequence(sequence);

						// 5. convert to string and count character count --
						// NOTE: some pages are not encoded in utf-8, but there
						// is no charset indicator, so assume utf-8 for all for now
						String pageContentsString = new String(pageContents, "utf-8");
						pageRecord.setCharacterCount(pageContentsString.length());
						volumeCharacterCount += pageContentsString.length();

						// 6. add page content into batch
						Insert insertStmt = QueryBuilder.insertInto(columnFamilyName);
						if(baseVersion == -1) {
							insertStmt.value("volumeID", volumeId)
							.value("version", VERSION_NO)
							.value("sequence", pageRecord.getSequence())
							.value("isBase", true)
							.value("isRemoved", false)
							.value("byteCount", pageRecord.getByteCount())
							.value("characterCount", pageRecord.getCharacterCount())
							.value("contents", pageContentsString)
							.value("checksum", pageRecord.getChecksum())
							.value("checksumType", pageRecord.getChecksumType())
							.value("pageNumberLabel", pageRecord.getLabel());
						} else {
							String[] contentLabelPair = sequenceToContentLabelPairMap.get(pageRecord.getSequence());
							if(contentLabelPair == null) {
								pwX.println(volumeId + "#" + pageRecord.getSequence() + " has base version " + baseVersion + ". but content label pair is null");
								pwX.flush();
								return false; // if there is not original page, which can happen when the new volume has more pages...
							}
							String originalText = contentLabelPair[0];
							String originalLabel = contentLabelPair[1];
							if(originalLabel == null && pageRecord.getLabel() == null) {
								System.out.println("label null for both original and current: " + volumeId + "#" + pageRecord.getSequence() );
								pwX.println("label null for both original and current: " + volumeId + "#" + pageRecord.getSequence() ); pwX.flush();
							} else if ((originalLabel == null && pageRecord.getLabel() != null)
									|| (originalLabel != null && pageRecord.getLabel() == null) 
									||!originalLabel.equals(pageRecord.getLabel())) {
								pwLabelMismatch.println(volumeId + "#" + pageRecord.getSequence() + " old: " + originalLabel + " new: " + pageRecord.getLabel());
								pwLabelMismatch.flush();
								return false;
							}
							String diffText = MyersDiffTool.getDiffString(originalText, pageContentsString);
							if(!diffText.equals("")) {
							//	System.out.println("**revised page: " + volumeId + "#" + sequence);
								pwHasDiff.println(volumeId + '\t' + pageRecord.getSequence() + '\t' + originalText.getBytes().length + '\t' + pageContentsString.getBytes().length + '\t' + diffText.getBytes().length);
								pwHasDiff.flush();
							}
							insertStmt.value("volumeID", volumeId)
							.value("version", VERSION_NO)
							.value("sequence", pageRecord.getSequence())
							.value("isBase", false)
							.value("isRemoved", false)
							.value("byteCount", pageRecord.getByteCount())
							.value("characterCount", pageRecord.getCharacterCount())
							.value("contents", diffText)
							.value("checksum", pageRecord.getChecksum())
							.value("checksumType", pageRecord.getChecksumType())
							.value("pageNumberLabel", pageRecord.getLabel());
							
							pwStats.println(volumeId + "\t" + pageRecord.getSequence() + "\t" + originalText.getBytes().length + "\t" + pageContentsString.getBytes().length + '\t' + diffText.getBytes().length);
							pwStats.flush();
						}
						
						batchStmt.add(insertStmt);
						if (i == 0) {
							firstPageInsert = insertStmt;
						}
						i++;
					}
				}
				zis.close();
				// 7. add static columns/fields into the first page
				if (firstPageInsert != null) {
					//ByteBuffer zipBinaryContent = getByteBuffer(volumeZipFile);
					if(baseVersion == -1) {
						firstPageInsert//.value("isRemoved", false)
				    //	.value("isBase", true)
						.value("baseVersion", VERSION_NO)
						.value("volumeByteCount", volumeByteCount)
						.value("volumeCharacterCount", volumeCharacterCount);
					} else {
						firstPageInsert//.value("isRemoved", false)
					//	.value("isBase", false)
						//.value("baseVersion", baseVersion)
						.value("volumeByteCount", volumeByteCount)
						.value("volumeCharacterCount", volumeCharacterCount);
					}
				}
				// 8. then push the volume into cassandra
				//System.out.println("VVVVVVVEEEERRSSSIIOONNN:" + VERSION_NO);
				cassandraManager.execute(batchStmt);
			} catch (IOException e) {
				log.error("IOException getting entry from ZIP " + volumeZipFile.getAbsolutePath(), e);
				return false;
			} catch (WriteFailureException e) {
				log.error("write failure for " + volumeId + ": " + e.getMessage());
				return false;
			}
			volumeAdded = true;
			return volumeAdded;
	//	} else return false;
	}
	
	private int baseVersion(String volumeId) throws UnexpectedResultSizeException {
		Statement select = QueryBuilder.select()/*.column("volumeid")*/.column("version")
				.from(cassandraManager.getKeySpace(), columnFamilyName)
				.where(QueryBuilder.eq("volumeid", volumeId))
				.and(QueryBuilder.eq("isBase", true))
				.limit(1);
		ResultSet result = cassandraManager.execute(select);
		System.out.println(select.toString());
		List<Row> rows = result.all();
		if(rows.size() > 1) {
			System.out.println("error: more than 1 base version for " + volumeId);
			log.error("error: more than 1 base version for " + volumeId);
			throw new UnexpectedResultSizeException("error: more than 1 base version for " + volumeId);
		} else if(rows.size() == 0) {
			return -1;
		} else {
			if(rows.get(0).isNull("version")) return -1;
			else return rows.get(0).getInt("version");
		}
		
	}

	private Map<String, String[]> getBasePages(String volumeId, int baseVersion) {
		Statement select = QueryBuilder.select().column("contents").column("volumeid").column("sequence").column("version").column("pageNumberLabel")
				.from(cassandraManager.getKeySpace(), columnFamilyName).where(QueryBuilder.eq("volumeid", volumeId))
				.and(QueryBuilder.eq("version", baseVersion));
				//.and(QueryBuilder.eq("sequence", sequence))
				//.and(QueryBuilder.eq("pageNumberLabel", pageNumberLabel));
		ResultSet result = cassandraManager.execute(select);
		Map<String, String[]> sequenceToContentLabelPairMap = new HashMap<String, String[]>();
		List<Row> rows = result.all();
		if(rows.size() == 0) {
			log.error("no base version for " + volumeId);
			System.out.println("error: no base version for " + volumeId);
			return null;
		} else {
			for(Row row : rows) {
				String sequence = row.getString("sequence");
				String text = row.getString("contents");
				String label = row.getString("pageNumberLabel");
				sequenceToContentLabelPairMap.put(sequence, new String[] {text, label});
			}
			return sequenceToContentLabelPairMap;
			/*System.out.println(rows.get(0).getString("pageNumberLabel"));
			if(!rows.get(0).getString("pageNumberLabel").equals(pageNumberLabel)) {
				pwMismatchedSequence.println(volumeId); pwMismatchedSequence.flush();
			}
			return text;*/
		}
	}
	
	public static void main(String[] args) throws Exception {
		CassandraVersionedIngester ingester = new CassandraVersionedIngester();
		int baseVersion = ingester.baseVersion("nyp.33433084033277");
		System.out.println("baseVersion is: " + baseVersion);
		System.out.println("==========================");
		//String basePageText = ingester.getBasePageContent("nyp.33433084033277", "00000002", baseVersion, "null");
		//System.out.println(basePageText);
		
		CassandraManager.getInstance().shutdown();
		
	}
	
	private ByteBuffer getByteBuffer(File file) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buffer = new byte[32767];
		
		try {
			InputStream is = new FileInputStream(file);
			int read = -1;
			while((read = is.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
			is.close();
			
			byte[] bytes = bos.toByteArray();
			ByteBuffer bf = ByteBuffer.wrap(bytes);
			return bf;
		} catch (FileNotFoundException e) {
			log.error(file.getAbsolutePath() + " is not found");
		} catch (IOException e) {
			log.error("IOException while attempting to read " + file.getName());
		}
		return null;
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
