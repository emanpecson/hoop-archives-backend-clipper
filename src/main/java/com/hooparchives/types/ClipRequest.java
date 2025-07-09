package com.hooparchives.types;

import java.util.List;
import java.util.Optional;

public class ClipRequest {
	public String clipId;
	public List<String> tags;
	public Float startTime;
	public Float endTime;
	public Float highlightTime;
	public String teamBeneficiary;
	public Optional<OffensivePlay> offense;
	public Optional<DefensivePlay> defense;
}
