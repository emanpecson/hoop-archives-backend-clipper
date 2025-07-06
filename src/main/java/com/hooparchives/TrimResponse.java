package com.hooparchives;

import java.util.List;

public class TrimResponse {
	public boolean success;
	public String errorMessage; // null if success
	public List<String> clipUrls;
	public String thumbnailUrl;

	public TrimResponse(List<String> clipUrls, String thumbnailUrl) {
		this.success = true;
		this.clipUrls = clipUrls;
		this.thumbnailUrl = thumbnailUrl;
		this.errorMessage = null;
	}

	public TrimResponse(String errorMessage) {
		this.success = false;
		this.clipUrls = null;
		this.thumbnailUrl = null;
		this.errorMessage = errorMessage;
	}
}
