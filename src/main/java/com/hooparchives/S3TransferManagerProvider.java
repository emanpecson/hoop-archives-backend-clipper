package com.hooparchives;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

public class S3TransferManagerProvider {
	private static final S3TransferManager transferManager = buildTransferManager();

	public static S3TransferManager buildTransferManager() {
		S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
				.multipartEnabled(true)
				.credentialsProvider(DefaultCredentialsProvider.create())
				.region(Region.US_WEST_2)
				.build();

		return S3TransferManager.builder()
				.s3Client(s3AsyncClient)
				.build();
	}

	public static S3TransferManager getTransferManager() {
		return transferManager;
	}
}
