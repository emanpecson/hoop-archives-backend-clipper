package com.hooparchives;

import java.util.List;

public class UploadResponse {
	public boolean success;
	public String errorMessage; // null if success
	public List<String> clipUrls;
	public String thumbnailUrl;

	public UploadResponse(List<String> clipUrls, String thumbnailUrl) {
		this.success = true;
		this.clipUrls = clipUrls;
		this.thumbnailUrl = thumbnailUrl;
		this.errorMessage = null;
	}

	public UploadResponse(String errorMessage) {
		this.success = false;
		this.clipUrls = null;
		this.thumbnailUrl = null;
		this.errorMessage = errorMessage;
	}
}
