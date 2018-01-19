package edu.indiana.d2i.zong.ingest.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import difflib.PatchFailedException;

public class MyersDiffTool {
	private static List<String> fileToLines(String filename) {
		List<String> lines = new LinkedList<String>();
		String line = "";
		try {
			BufferedReader in = new BufferedReader(new FileReader(filename));
			while ((line = in.readLine()) != null) {
				lines.add(line);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
	private static List<String> stringTextToLines(String text) {
		List<String> lines = new LinkedList<String>();
		String line = "";
		try {
			BufferedReader in = new BufferedReader(new StringReader(text));
			while ((line = in.readLine()) != null) {
				lines.add(line);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
	
	
	static PrintWriter pw ;
	
	static {
		try {
			pw = new PrintWriter("delta-count-per-page.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * get the diff between originalText and revisedText
	 * @param originalText
	 * @param revisedText
	 * @return the diff in string
	 */
	public static String getDiffString(String originalText, String revisedText) {
		List<String> originalLines = stringTextToLines(originalText);
		List<String> revisedLines = stringTextToLines(revisedText);
		Patch patch = DiffUtils.diff(originalLines, revisedLines);
		List<String> diffStrs = DiffUtils.generateUnifiedDiff("original.txt", "revise.txt", originalLines, patch, 0);
		
		int size = 0;
		int diffCount =  patch.getDeltas().size();
		/*for(Delta delta : patch.getDeltas()) {
			System.out.println(delta.getOriginal().toString());
			size += delta.getOriginal().toString().getBytes().length;
			System.out.println(delta.getRevised().toString());
			size += delta.getRevised().toString().getBytes().length;
			diffCount++;
		}*/
		if(diffCount != 0) {
			pw.println(diffCount); pw.flush();
		}
		
		//System.out.println("raw delta bytes: " + size);
		/*SerializedPatch serializedPatch = new SerializedPatch(patch);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("patch.txt"));
			oos.writeObject(serializedPatch);
			oos.flush();oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}*/

	    StringBuilder sb = new StringBuilder();
	    for(int i=0; i<diffStrs.size(); i++) {
	    	sb.append(diffStrs.get(i));
	    	if(i < diffStrs.size()-1) {
	    		sb.append('\n');
	    	}
	    }
	    return sb.toString();
	}
	
	/**
	 * get revised string by applying diffText/delta to originalText. 
	 * @param originalText
	 * @param diffText
	 * @return the revised version of text. e.g. updated OCR page
	 * @throws PatchFailedException 
	 */
	public static String getRevisedString(String originalText, String diffText) throws PatchFailedException {
		List<String> originalLines = stringTextToLines(originalText);
		List<String> diffLines = stringTextToLines(diffText);
		Patch patch = DiffUtils.parseUnifiedDiff(diffLines);
	    List<?> revisedLines = DiffUtils.patch(originalLines, patch);
	    StringBuilder sb = new StringBuilder();
	    for(int i=0; i<revisedLines.size(); i++) {
	    	sb.append(revisedLines.get(i));
	    	if(i < revisedLines.size()-1) {
	    		sb.append('\n');
	    	}
	    }
		return sb.toString();
	}

	private static String concat(List<String> strs) {
		StringBuilder sb = new StringBuilder();
	    for(int i=0; i<strs.size(); i++) {
	    	sb.append(strs.get(i));
	    	if(i < strs.size()-1) {
	    		sb.append('\n');
	    	}
	    }
	    return sb.toString();
	}
	
	public static void main(String[] args) throws PatchFailedException, FileNotFoundException {
		List<String> original = fileToLines("C:/zong/new-00000793.txt");
        List<String> revised  = fileToLines("C:/zong/old-00000793.txt");
        String originalText = concat(original);
        String revisedText = concat(revised);
        String diffStr = getDiffString(originalText, revisedText);
        String recoveredRevisedText = getRevisedString(originalText, diffStr);
        System.out.println(diffStr.equals(""));
        System.out.println(diffStr);
        System.out.println("-----------------------");
        System.out.println(recoveredRevisedText);
        
        System.out.println("originalText bytes: " + originalText.getBytes().length);
        System.out.println("revisedText bytes: " + revisedText.getBytes().length);
        System.out.println("delta bytes: " + diffStr.getBytes().length);
     //   System.out.println(diffStr);
		/*
		List<String> original = fileToLines("/home/zong/license.txt");
        List<String> revised  = fileToLines("/home/zong/revised-lic.txt");

        // Compute diff. Get the Patch object. Patch is the container for computed deltas.
        Patch patch = DiffUtils.diff(original, revised);
        
        //List<Delta> deltaList = patch.getDeltas();

        for (Delta delta: deltaList) {
        	// System.out.println(delta.getRevised());
            System.out.println(delta);
        }

        List<String> res = DiffUtils.generateUnifiedDiff("original.txt", "revise.txt", original, patch, 0);
        System.out.println("====================");
        PrintWriter pw = new PrintWriter("diff.txt");
        for(String s : res) {
        	System.out.println(s);
        	pw.println(s);
        }
        pw.flush();pw.close();
        System.out.println("====================");
        List<String> originalCopy = new LinkedList<String>(original);
        
        List<String> patchRecoverLines  = fileToLines("diff.txt");
        Patch patchRecover = DiffUtils.parseUnifiedDiff(patchRecoverLines);
        List<?> recoveredContents = DiffUtils.patch(originalCopy, patchRecover);
        System.out.println(patchRecoverLines.size());
        System.out.println("-----------------------------");
        StringBuilder sb = new StringBuilder();
        int i=0;
        for(Object s : recoveredContents) {
        	System.out.println(s instanceof String);
        	sb.append(s);
        	if(i < recoveredContents.size()-1) {
        		sb.append('\n');
        	}
        	i++;
        }
        
        System.out.println(sb.toString());
        
	*/}

}
