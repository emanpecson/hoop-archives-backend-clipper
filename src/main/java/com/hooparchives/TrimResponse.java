package com.hooparchives;

import java.util.List;

public class TrimResponse {
	public List<String> clipUrls;
	public String thumbnailUrl;

	TrimResponse(List<String> clipUrls, String thumbnailUrl) {
		this.clipUrls = clipUrls;
		this.thumbnailUrl = thumbnailUrl;
	}
}
