package com.hooparchives.types;

import java.util.Date;
import java.util.List;

public class GameRequest {
	public String leagueId;
	public String gameId;
	public String key; // game title + extension
	public Date date;

	public List<ClipRequest> clipRequests;
}
