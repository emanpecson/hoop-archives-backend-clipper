package com.hooparchives;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hooparchives.types.ClipRequest;
import com.hooparchives.types.DefensivePlay;
import com.hooparchives.types.GameStatusEnum;
import com.hooparchives.types.OffensivePlay;
import com.hooparchives.types.Player;
import com.hooparchives.types.UploadRequest;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public class DdbHandler {
	private static final DynamoDbClient ddbClient = buildDdbClient();
	private static final String gamesTable = System.getenv("AWS_DDB_GAMES_TABLE");
	private static final String clipsTable = System.getenv("AWS_DDB_CLIPS_TABLE");

	private static DynamoDbClient buildDdbClient() {
		return DynamoDbClient.builder()
				.region(Region.US_WEST_2)
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build();
	}

	// * Convert `Player` to map
	public static Map<String, AttributeValue> toAttributeValue(Player player) {
		Map<String, AttributeValue> map = new HashMap<>();
		map.put("leagueId", AttributeValue.fromS(player.leagueId));
		map.put("fullName", AttributeValue.fromS(player.fullName));
		map.put("playerId", AttributeValue.fromS(player.playerId));
		map.put("firstName", AttributeValue.fromS(player.firstName));
		map.put("lastName", AttributeValue.fromS(player.lastName));
		map.put("imageUrl", AttributeValue.fromS(player.imageUrl));
		return map;
	}

	// * Convert list of `Player`s to list of maps
	public static List<AttributeValue> toAttributeValue(List<Player> players) {
		List<AttributeValue> listPlayers = players.stream()
				.map(DdbHandler::toAttributeValue)
				.map(AttributeValue::fromM)
				.collect(Collectors.toList());
		return listPlayers;
	}

	/**
	 * Create GameClips object in DynamoDB
	 * 
	 * @param uploadReq Common clip data
	 * @param clipReq   Unique clip data
	 * @param clipUrl   Generated S3 URL
	 * @throws Exception
	 */
	public void createGameClip(UploadRequest uploadReq, ClipRequest clipReq, String clipUrl) throws Exception {
		// build game clip
		Map<String, AttributeValue> gameClip = new HashMap<>();

		// common clip attributes
		gameClip.put("leagueId", AttributeValue.fromS(uploadReq.leagueId));
		gameClip.put("gameTitle", AttributeValue.fromS(uploadReq.gameTitle));
		gameClip.put("date", AttributeValue.fromS(uploadReq.date.toString()));

		// unique clip attributes
		gameClip.put("clipId", AttributeValue.fromS(clipReq.clipId));
		gameClip.put("tags", AttributeValue.fromSs(clipReq.tags));
		gameClip.put("startTime", AttributeValue.fromN(clipReq.startTime.toString()));
		gameClip.put("endTime", AttributeValue.fromN(clipReq.endTime.toString()));
		gameClip.put("highlightTime", AttributeValue.fromN(clipReq.highlightTime.toString()));
		gameClip.put("teamBeneficiary", AttributeValue.fromS(clipReq.teamBeneficiary));
		gameClip.put("teamBeneficiary", AttributeValue.fromS(clipReq.teamBeneficiary));
		gameClip.put("url", AttributeValue.fromS(clipUrl));

		// conditional `offense` play
		if (clipReq.offense != null && clipReq.offense.isPresent()) {
			OffensivePlay offensivePlay = clipReq.offense.get();
			Map<String, AttributeValue> offense = new HashMap<>();

			offense.put("pointsAdded", AttributeValue.fromN(offensivePlay.pointsAdded.toString()));
			offense.put("playerScoring", AttributeValue.fromM(toAttributeValue(offensivePlay.playerScoring)));

			if (offensivePlay.playerAssisting != null && offensivePlay.playerAssisting.isPresent()) {
				Player assister = offensivePlay.playerAssisting.get();
				offense.put("playerAssisting", AttributeValue.fromM(toAttributeValue(assister)));
			}

			if (offensivePlay.playersDefending != null && offensivePlay.playersDefending.isPresent()) {
				List<Player> defenders = offensivePlay.playersDefending.get();
				offense.put("playersDefending", AttributeValue.fromL(toAttributeValue(defenders)));
			}

			gameClip.put("offense", AttributeValue.fromM(offense));
		}

		// conditional `defense` play
		else if (clipReq.defense != null && clipReq.defense.isPresent()) {
			DefensivePlay defensivePlay = clipReq.defense.get();
			Map<String, AttributeValue> defense = new HashMap<>();

			defense.put("playerDefending", AttributeValue.fromM(toAttributeValue(defensivePlay.playerDefending)));
			defense.put("playerStopped", AttributeValue.fromM(toAttributeValue(defensivePlay.playerStopped)));
		}

		PutItemRequest req = PutItemRequest.builder()
				.tableName(clipsTable)
				.item(gameClip)
				.build();

		PutItemResponse res = ddbClient.putItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error creating clip in DDB: " + clipReq.clipId);
		}
	}

	/**
	 * Update Game status in DynamoDB
	 * 
	 * @param uploadReq
	 * @param status    update
	 * @throws Exception
	 */
	public void updateGameStatus(UploadRequest uploadReq, GameStatusEnum status) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(uploadReq.leagueId));
		key.put("title", AttributeValue.fromS(uploadReq.gameTitle));

		// escape reserved ddb word "status"
		Map<String, String> expressionAttributeNames = new HashMap<>();
		expressionAttributeNames.put("#status", "status");

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":status", AttributeValue.fromS(status.toString()));

		UpdateItemRequest updateReq = UpdateItemRequest
				.builder()
				.tableName(gamesTable)
				.key(key)
				.expressionAttributeNames(expressionAttributeNames)
				.expressionAttributeValues(expressionAttributeValues)
				.updateExpression("SET #status = :status")
				.build();

		UpdateItemResponse res = ddbClient.updateItem(updateReq);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error updating game status: " + uploadReq.gameTitle);
		}
	}

	/**
	 * Update Game thumbnail in DynamoDB
	 * 
	 * @param uploadReq
	 * @param thumbnailUrl Attribute to update
	 * @throws Exception
	 */
	public void updateGameThumbnail(UploadRequest uploadReq, String thumbnailUrl) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(uploadReq.leagueId));
		key.put("title", AttributeValue.fromS(uploadReq.gameTitle));

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":thumbnailUrl", AttributeValue.fromS(thumbnailUrl));

		UpdateItemRequest req = UpdateItemRequest
				.builder()
				.tableName(gamesTable)
				.key(key)
				.updateExpression("SET thumbnailUrl = :thumbnailUrl")
				.expressionAttributeValues(expressionAttributeValues)
				.build();

		UpdateItemResponse res = ddbClient.updateItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error updating game thumbnail: " + uploadReq.gameTitle);
		}
	}
}