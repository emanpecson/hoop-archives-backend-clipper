package com.hooparchives;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hooparchives.types.ClipRequest;
import com.hooparchives.types.DefensivePlay;
import com.hooparchives.types.UploadStatusEnum;
import com.hooparchives.types.OffensivePlay;
import com.hooparchives.types.Player;
import com.hooparchives.types.ReelRequest;
import com.hooparchives.types.GameRequest;

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
	private static final String reelsTable = System.getenv("AWS_DDB_REELS_TABLE");

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
	 * @param gameReq   Common clip data
	 * @param clipReq   Unique clip data
	 * @param clipUrl   Generated S3 URL
	 * @param bucketKey S3 key
	 * @throws Exception
	 */
	public void createGameClip(GameRequest gameReq, ClipRequest clipReq, String clipUrl, String bucketKey)
			throws Exception {
		// build game clip
		Map<String, AttributeValue> gameClip = new HashMap<>();

		List<AttributeValue> tagList = clipReq.tags.stream()
				.map(AttributeValue::fromS)
				.collect(Collectors.toList());

		// common clip attributes
		gameClip.put("leagueId", AttributeValue.fromS(gameReq.leagueId));
		gameClip.put("gameId", AttributeValue.fromS(gameReq.gameId));
		gameClip.put("date", AttributeValue.fromS(gameReq.date.toString()));

		// unique clip attributes
		gameClip.put("clipId", AttributeValue.fromS(clipReq.clipId));
		gameClip.put("bucketKey", AttributeValue.fromS(bucketKey));
		gameClip.put("tags", AttributeValue.fromL(tagList));
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

	// ------------------------------------------------------------------------------------------------------------------
	// //

	/**
	 * Update Game upload status in DynamoDB
	 * 
	 * @param gameReq
	 * @param status  to update
	 * @throws Exception
	 */
	public void setGameUploadStatus(GameRequest gameReq, UploadStatusEnum status) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(gameReq.leagueId));
		key.put("gameId", AttributeValue.fromS(gameReq.gameId));

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
			throw new Exception("Error updating Game upload status: " + gameReq.gameId);
		}
	}

	/**
	 * Update Game thumbnail in DynamoDB
	 * 
	 * @param gameReq
	 * @param thumbnailUrl to update
	 * @throws Exception
	 */
	public void setGameThumbnailUrl(GameRequest gameReq, String thumbnailUrl) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(gameReq.leagueId));
		key.put("gameId", AttributeValue.fromS(gameReq.gameId));

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
			throw new Exception("Error updating Game thumbnail: " + gameReq.gameId);
		}
	}

	/**
	 * Update Game source in DynamoDB
	 * 
	 * @param gameReq
	 * @param sourceUrl to update
	 * @throws Exception
	 */
	public void setGameSourceUrl(GameRequest gameReq, String sourceUrl) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(gameReq.leagueId));
		key.put("gameId", AttributeValue.fromS(gameReq.gameId));

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":sourceUrl", AttributeValue.fromS(sourceUrl));

		UpdateItemRequest req = UpdateItemRequest
				.builder()
				.tableName(gamesTable)
				.key(key)
				.updateExpression("SET sourceUrl = :sourceUrl")
				.expressionAttributeValues(expressionAttributeValues)
				.build();

		UpdateItemResponse res = ddbClient.updateItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error updating Game source URL: " + gameReq.gameId);
		}
	}

	// ------------------------------------------------------------------------------------------------------------------

	/**
	 * Update Reel upload status in DynamoDB
	 * 
	 * @param reelReq
	 * @param status  to update
	 * @throws Exception
	 */
	public void setReelUploadStatus(ReelRequest reelReq, UploadStatusEnum status) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(reelReq.leagueId));
		key.put("reelId", AttributeValue.fromS(reelReq.reelId));

		// escape reserved ddb word "status"
		Map<String, String> expressionAttributeNames = new HashMap<>();
		expressionAttributeNames.put("#status", "status");

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":status", AttributeValue.fromS(status.toString()));

		UpdateItemRequest updateReq = UpdateItemRequest
				.builder()
				.tableName(reelsTable)
				.key(key)
				.expressionAttributeNames(expressionAttributeNames)
				.expressionAttributeValues(expressionAttributeValues)
				.updateExpression("SET #status = :status")
				.build();

		UpdateItemResponse res = ddbClient.updateItem(updateReq);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error updating Reel upload status: " + reelReq.reelId);
		}
	}

	/**
	 * Update Reel thumbnail in DynamoDB
	 * 
	 * @param reelReq
	 * @param thumbnailUrl to update
	 * @throws Exception
	 */
	public void setReelThumbnailUrl(ReelRequest reelReq, String thumbnailUrl) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(reelReq.leagueId));
		key.put("reelId", AttributeValue.fromS(reelReq.reelId));

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":thumbnailUrl", AttributeValue.fromS(thumbnailUrl));

		UpdateItemRequest req = UpdateItemRequest
				.builder()
				.tableName(reelsTable)
				.key(key)
				.updateExpression("SET thumbnailUrl = :thumbnailUrl")
				.expressionAttributeValues(expressionAttributeValues)
				.build();

		UpdateItemResponse res = ddbClient.updateItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error updating Reel thumbnail: " + reelReq.reelId);
		}
	}

	/**
	 * Update Reel source in DynamoDB
	 * 
	 * @param reelReq
	 * @param sourceUrl to update
	 * @throws Exception
	 */
	public void setReelSourceUrl(ReelRequest reelReq, String sourceUrl) throws Exception {
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("leagueId", AttributeValue.fromS(reelReq.leagueId));
		key.put("reelId", AttributeValue.fromS(reelReq.reelId));

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
		expressionAttributeValues.put(":sourceUrl", AttributeValue.fromS(sourceUrl));

		UpdateItemRequest req = UpdateItemRequest
				.builder()
				.tableName(reelsTable)
				.key(key)
				.updateExpression("SET sourceUrl = :sourceUrl")
				.expressionAttributeValues(expressionAttributeValues)
				.build();

		UpdateItemResponse res = ddbClient.updateItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error updating Reel source URL: " + reelReq.reelId);
		}
	}
}