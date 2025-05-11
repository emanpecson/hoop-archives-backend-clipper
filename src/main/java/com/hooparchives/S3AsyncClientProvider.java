package com.hooparchives;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3AsyncClientProvider {
	private static final S3AsyncClient s3AsyncClient = buildS3AsyncClient();

	private static S3AsyncClient buildS3AsyncClient() {
		return S3AsyncClient.crtBuilder()
				.region(Region.US_WEST_2)
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build();
	}

	public static S3AsyncClient getClient() {
		return s3AsyncClient;
	}
}
