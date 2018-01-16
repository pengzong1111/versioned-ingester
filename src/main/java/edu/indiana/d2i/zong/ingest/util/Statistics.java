package edu.indiana.d2i.zong.ingest.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

/**
 * for the oldest list of volumes, get size in bytes, number of pages and preferably page label and sequence.  
 * @author zongpeng
 *
 */
public class Statistics {
    enum Source {
    	full_set, ht_text, htmnt, dir;
    }
    
    private static Source src;
	public static void collectStats(List<String> volumeIds) throws FileNotFoundException {
		System.out.println("overall number of volumes: " + volumeIds.size());
		PrintWriter pw = new PrintWriter("stats.txt");
		for(String id: volumeIds) {
			List<String> stats = Tools.getStats(id, src);
			if(stats == null) {
				System.out.println("null stats: " + id);
				continue;
			}
			if(stats.isEmpty()) {
				System.out.println("empty stats: " + id);
				continue;
			}
			StringBuilder sb = new StringBuilder();
			sb.append(id).append('\t');
			int i=0;
			for(String s : stats) {
				sb.append(s);
				if(i<stats.size()-1) {
					sb.append('\t');
				}
				i++;
			}
			pw.println(sb.toString());
			pw.flush();
		}
		pw.flush();
		pw.close();
		System.out.println("stats collction done.");
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		List<String> volumeIds = Tools.getVolumeIds(new File(args[1]));
		src = Source.valueOf(args[0]);
		collectStats(volumeIds);
	}

}
