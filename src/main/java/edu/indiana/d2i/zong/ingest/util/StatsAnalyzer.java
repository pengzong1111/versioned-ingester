package edu.indiana.d2i.zong.ingest.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class StatsAnalyzer {

	public static void main(String[] args) throws IOException {
	//	File oldStatsFile = new File("C:/Users/zongpeng/Documents/stats-ht_text.txt");
	//	File newStatsFile = new File("C:/Users/zongpeng/Documents/stats-silvermaple.txt");
		File oldStatsFile = new File(args[0]);
		File newStatsFile = new File(args[1]);
		
		Map<String, Map<String, String>> volumeIdToStatsMapOld = new HashMap<String, Map<String, String>>();
		Map<String, Map<String, String>> volumeIdToStatsMapNew = new HashMap<String, Map<String, String>>();
		
		loadStats(oldStatsFile, volumeIdToStatsMapOld);
		loadStats(newStatsFile, volumeIdToStatsMapNew);
		System.out.println("files loaded");
		PrintWriter pw = new PrintWriter("volumesWithSamePageCount.txt");
		PrintWriter idPw = new PrintWriter("ids.txt");
		PrintWriter pwDiffSize = new PrintWriter("volumesWithSamePageCountDiffSize.txt");
		for(String id : volumeIdToStatsMapNew.keySet()) {
			if(volumeIdToStatsMapOld.containsKey(id)) {
				int oldPageCount = Integer.valueOf(volumeIdToStatsMapOld.get(id).get("pageCount"));
				int newPageCount = Integer.valueOf(volumeIdToStatsMapNew.get(id).get("pageCount"));
				if(oldPageCount == newPageCount) {
					pw.println(id + '\t' + newPageCount + '\t' + volumeIdToStatsMapOld.get(id).get("size") + '\t' + volumeIdToStatsMapNew.get(id).get("size"));
					pw.flush();
					idPw.println(id);
					idPw.flush();
					int oldSize = Integer.valueOf(volumeIdToStatsMapOld.get(id).get("size"));
					int newSize = Integer.valueOf(volumeIdToStatsMapNew.get(id).get("size"));
					if(oldSize != newSize) {
						pwDiffSize.println(id);
						pwDiffSize.flush();
					}
				}
			}
		}
		pw.flush();idPw.flush();
		pw.close();idPw.close();
		pwDiffSize.flush(); pwDiffSize.close();
	}

	private static void loadStats(File statsFile, Map<String, Map<String, String>> volumeIdToStatsMap) throws IOException {
		System.out.println("loading " + statsFile.getAbsolutePath());
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(statsFile)));
		String line = null;
		while((line = br.readLine()) != null) {
			String[] splits = line.split("\t");
		//	System.out.println(line);
			String volumeId = splits[0];
			String size = splits[1];
			String time = splits[2];
			String pageCount = splits[3];
			Map<String, String> map = new HashMap<String, String>();
			map.put("size", size);
			map.put("time", time);
			map.put("pageCount", pageCount);
			volumeIdToStatsMap.put(volumeId, map);
		}
		
		br.close();
	}

}
