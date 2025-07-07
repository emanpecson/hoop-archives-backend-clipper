package com.hooparchives;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbClientProvider {
	private static final DynamoDbClient ddbClient = buildDdbClient();

	private static DynamoDbClient buildDdbClient() {
		return DynamoDbClient.builder()
				.region(Region.US_WEST_2)
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build();
	}

	public static DynamoDbClient getClient() {
		return ddbClient;
	}
}