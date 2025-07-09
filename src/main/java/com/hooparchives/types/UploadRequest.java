package com.hooparchives.types;

import java.util.Date;
import java.util.List;

public class UploadRequest {
	public String leagueId;
	public String gameTitle;
	public String key; // game title + extension
	public Date date;

	public List<ClipRequest> clipRequests;
}
