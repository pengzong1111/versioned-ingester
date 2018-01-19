package edu.indiana.d2i.zong.ingest.util;

import java.io.Serializable;

import difflib.Patch;

public class SerializedPatch implements Serializable {
	private Patch patch;
	public SerializedPatch(Patch patch) {
		this.patch = patch;
	}

	public Patch getPatch() {
		
		return this.patch;
	}
}
