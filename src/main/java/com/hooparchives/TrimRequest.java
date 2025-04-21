package com.hooparchives;

import java.util.List;

public class TrimRequest {
	public String filename;
	public List<Clip> clips;

	public static class Clip {
		public String start;
		public String duration;
	}
}
