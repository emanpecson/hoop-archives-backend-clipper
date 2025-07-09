package com.hooparchives;

import java.nio.file.Path;

import com.amazonaws.services.lambda.runtime.Context;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

public class S3TransferManagerHandler {
	private static final S3TransferManager transferManager = buildTransferManager();
	private static final String bucket = System.getenv("AWS_S3_BUCKET_NAME");
	private static final String region = System.getenv("AWS_REGION");

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

	/**
	 * Downloads a video from S3 and stores it into ~/tmp/downloads/*
	 * 
	 * @param destinationPath Downloads folder with the filename
	 * @param key             Key of stored item
	 */
	public void downloadFile(Path destinationPath, String key) throws Exception {
		DownloadFileRequest req = DownloadFileRequest.builder()
				.getObjectRequest(b -> b.bucket(bucket).key(key))
				.destination(destinationPath)
				.build();

		FileDownload downloadFile = transferManager.downloadFile(req);
		CompletedFileDownload download = downloadFile.completionFuture().join();
		SdkHttpResponse res = download.response().sdkHttpResponse();

		if (!res.isSuccessful()) {
			throw new Exception("Download failed with status code: " + res.statusCode());
		}
	}

	/**
	 * Upload clip to S3 "Uploads" bucket
	 * 
	 * @param clipPath Path to file to upload
	 * @param key      Key of item to store
	 * @return Clip url
	 * @throws Exception
	 */
	public String upload(Path clipPath, String key, Context context) throws Exception {
		UploadFileRequest req = UploadFileRequest.builder()
				.putObjectRequest(b -> b.bucket(bucket).key(key))
				.source(clipPath)
				.build();

		FileUpload upload = transferManager.uploadFile(req);
		CompletedFileUpload uploadResult = upload.completionFuture().join();
		SdkHttpResponse res = uploadResult.response().sdkHttpResponse();

		if (!res.isSuccessful()) {
			throw new Exception("Invalid S3 put request");
		}

		String url = "https://" + bucket + ".s3." + region + ".amazonaws.com/" + key;
		context.getLogger().log("Successful put request: " + url);

		return url;
	}
}
