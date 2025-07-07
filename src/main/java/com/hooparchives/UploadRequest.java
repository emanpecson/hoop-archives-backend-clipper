package com.hooparchives;

import java.util.List;

public class UploadRequest {
	public String key;
	public List<Clip> clips;

	public static class Clip {
		public String start;
		public String duration;
	}
}
