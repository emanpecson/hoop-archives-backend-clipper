package com.hooparchives.types;

import java.util.List;
import java.util.Optional;

public class OffensivePlay {
	public Integer pointsAdded;
	public Player playerScoring;
	public Optional<Player> playerAssisting;
	public Optional<List<Player>> playersDefending;
}
