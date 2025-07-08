package com.hooparchives;

import com.amazonaws.services.lambda.runtime.Context;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public class S3AsyncClientHandler {
	private static final S3AsyncClient s3AsyncClient = buildS3AsyncClient();
	private static final String bucket = System.getenv("AWS_S3_BUCKET_NAME");

	private static S3AsyncClient buildS3AsyncClient() {
		return S3AsyncClient.crtBuilder()
				.region(Region.US_WEST_2)
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build();
	}

	/**
	 * Delete file object from S3 bucket. After creating the clips, the video
	 * is no longer necessary. As we'll be able to reference a game by its clips.
	 * 
	 * @param key Key of stored item to remove
	 * @throws RuntimeException
	 */
	public void delete(String key, Context context) throws RuntimeException {
		DeleteObjectRequest req = DeleteObjectRequest.builder()
				.bucket(bucket)
				.key(key)
				.build();

		s3AsyncClient.deleteObject(req).join(); // waits until delete is complete
		context.getLogger().log("Deleted S3 object: " + key);
	}
}
