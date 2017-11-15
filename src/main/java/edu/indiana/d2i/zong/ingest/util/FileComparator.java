package edu.indiana.d2i.zong.ingest.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import difflib.Chunk;
import difflib.Delta;
import difflib.Delta.TYPE;
import difflib.DiffUtils;
import difflib.Patch;

public class FileComparator {
	private File original;
	private File revised;
	
	public FileComparator(File original, File revised) {
		this.original = original;
		this.revised = revised;
	}
	
	public List<Chunk> getChangesFromOrignal() throws IOException {
		return getChunksByType(Delta.TYPE.CHANGE);
	}

	public List<Chunk> getInsertsFromOriginal() throws IOException {
		return getChunksByType(Delta.TYPE.INSERT);
	}

	public List<Chunk> getDeletesFromOriginal() throws IOException {
		return getChunksByType(Delta.TYPE.DELETE);
	}

	private List<Chunk> getChunksByType(TYPE type) throws IOException {
		final List<Chunk> listOfChanges = new ArrayList<Chunk>();
        final List<Delta> deltas = getDeltas();
        for (Delta delta : deltas) {
            if (delta.getType() == type) {
                listOfChanges.add(delta.getRevised());
            }
        }
        return listOfChanges;
	}

	private List<Delta> getDeltas() throws IOException {
		 
        final List<String> originalFileLines = fileToLines(original);
        final List<String> revisedFileLines = fileToLines(revised);
 
        final Patch patch = DiffUtils.diff(originalFileLines, revisedFileLines);
        
        return patch.getDeltas();
    }
 
    private List<String> fileToLines(File file) throws IOException {
        final List<String> lines = new ArrayList<String>();
        String line;
        final BufferedReader in = new BufferedReader(new FileReader(file));
        while ((line = in.readLine()) != null) {
            lines.add(line);
        }
        in.close();
        return lines;
    }
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
