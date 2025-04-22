package com.hooparchives;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.hooparchives.TrimRequest.Clip;

import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

public class Clipper {
	private final S3TransferManager s3TransferManager;
	private final S3AsyncClient s3Client;
	private final DynamoDbClient ddbClient;

	private final String region = "us-west-2";
	private final String bucket = "hoop-archives-uploads";
	private final String gamesTable = "Games";
	private final String gameClipsTable = "GameClips";

	private final String downloadsPathname = "downloads";
	private final String clipsPathname = "clips";

	public Clipper() {
		this.s3TransferManager = S3TransferManagerProvider.getTransferManager();
		this.s3Client = S3AsyncClientProvider.getClient();
		this.ddbClient = DyanmoDbClientProvider.getClient();

	}

	/**
	 * Entry-point function that downloads the targeted video from S3 and creates
	 * clips to be uploaded into S3. These clips are then created individually into
	 * the GameClips table and are grouped by the Games table.
	 * 
	 * @param req Trim request
	 * @return Clip urls
	 * @throws Exception
	 * @throws IOException
	 */
	public ArrayList<String> handleTrimRequests(TrimRequest req) throws Exception, IOException {
		String gameId = "";
		ArrayList<String> clipUrls = new ArrayList<>();

		// download video
		Path downloadsPath = Paths.get(this.downloadsPathname, req.filename);
		this.downloadFile(req.filename, downloadsPath);

		// create a game to link with all clips
		gameId = this.ddbCreateGame(req.title);

		// process clips
		for (int i = 0; i < req.clips.size(); i++) {
			Clip clip = req.clips.get(i);
			Integer dotIndex = req.filename.indexOf(".");
			String clipName = req.filename.substring(0, dotIndex) + "_" + i + req.filename.substring(dotIndex);

			Path clipOutputPath = Paths.get(this.clipsPathname, clipName);

			this.trimClip(downloadsPath, clipOutputPath, clip, clipName);
			String clipUrl = this.s3UploadClip(clipOutputPath, clipName);
			this.ddbCreateClip(gameId, clipName, clipUrl);

			clipUrls.add(clipUrl);
		}

		// remove original video from downloads + s3 bucket
		this.s3RemoveVideo(req.filename);
		this.deleteDownload(req.filename);

		return clipUrls;
	}

	/**
	 * Downloads a video from S3 and stores it into ~/downloads/*
	 * 
	 * @param filename        Key of stored item
	 * @param destinationPath Downloads folder with the filename
	 */
	private void downloadFile(String filename, Path destinationPath) throws Exception {
		DownloadFileRequest req = DownloadFileRequest.builder()
				.getObjectRequest(b -> b.bucket(this.bucket).key(filename))
				.destination(destinationPath)
				.build();

		FileDownload downloadFile = this.s3TransferManager.downloadFile(req);
		CompletedFileDownload download = downloadFile.completionFuture().join();
		SdkHttpResponse res = download.response().sdkHttpResponse();

		if (!res.isSuccessful()) {
			throw new Exception("Download failed with status code: " + res.statusCode());
		}
	}

	/**
	 * Starts a process with FFMPEG to trim a video into a clip.
	 * 
	 * @param videoInputPath Path to video to edit
	 * @param clipOutputPath Path to store created clips
	 * @param clip           Clip with start/duration
	 * @param clipName       Name of clip to create
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws Exception
	 */
	private void trimClip(Path videoInputPath, Path clipOutputPath, Clip clip, String clipName)
			throws IOException, InterruptedException, Exception {
		ProcessBuilder processBuilder = new ProcessBuilder(
				"ffmpeg",
				"-i", videoInputPath.toString(),
				"-ss", clip.start,
				"-t", clip.duration,
				"-c", "copy", // copy codecs (w/o re-encoding)
				clipOutputPath.toString());

		System.out.println("Processing " + clipName + "...");
		Process process = processBuilder.start();
		int exitCode = process.waitFor();

		if (exitCode != 0) {
			throw new Exception("Process failed with exit code: " + exitCode);
		}
	}

	/**
	 * Upload clip to S3 "Uploads" bucket
	 * 
	 * @param clipPath Path to file to upload
	 * @param filename Key of item to store
	 * @return Clip url
	 * @throws Exception
	 */
	private String s3UploadClip(Path clipPath, String filename) throws Exception {
		UploadFileRequest req = UploadFileRequest.builder()
				.putObjectRequest(b -> b.bucket(this.bucket).key(filename))
				.source(clipPath)
				.build();

		FileUpload upload = this.s3TransferManager.uploadFile(req);
		CompletedFileUpload uploadResult = upload.completionFuture().join();
		SdkHttpResponse res = uploadResult.response().sdkHttpResponse();

		if (!res.isSuccessful()) {
			throw new Exception("Invalid S3 put request");
		}

		String url = "https://" + this.bucket + ".s3." + this.region + ".amazonaws.com/" + filename;
		System.out.println("Successful put request: " + url);

		return url;
	}

	/**
	 * Create Clip object in "GameClips" DynamoDB table
	 * 
	 * @param gameId   Parition key
	 * @param clipName Sort key: gameId + index
	 * @param clipUrl  S3 URL to reference
	 * @throws Exception
	 */
	private void ddbCreateClip(String gameId, String clipName, String clipUrl) throws Exception {

		HashMap<String, AttributeValue> obj = new HashMap<>();
		obj.put("gameId", AttributeValue.builder().s(gameId).build());
		obj.put("clipId", AttributeValue.builder().s(clipName).build());
		obj.put("clipUrl", AttributeValue.builder().s(clipUrl).build());

		PutItemRequest req = PutItemRequest.builder()
				.tableName(this.gameClipsTable)
				.item(null)
				.build();

		PutItemResponse res = this.ddbClient.putItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error creating clip in DDB: " + clipName);
		}
	}

	/**
	 * Create Game object in "Games" DynamoDB table
	 * 
	 * @param title Name of game
	 * @return Game id
	 * @throws Exception
	 */
	private String ddbCreateGame(String title) throws Exception {
		String uniqueGameId = UUID.randomUUID().toString();

		HashMap<String, AttributeValue> obj = new HashMap<>();
		obj.put("gameId", AttributeValue.builder().s(uniqueGameId).build());
		obj.put("title", AttributeValue.builder().s(title).build());
		obj.put("date", AttributeValue.builder().s(Instant.now().toString()).build());

		PutItemRequest req = PutItemRequest.builder()
				.tableName(this.gamesTable)
				.item(obj)
				.build();

		PutItemResponse res = this.ddbClient.putItem(req);

		if (!res.sdkHttpResponse().isSuccessful()) {
			throw new Exception("Error creating game in DDB: " + title);
		}

		return uniqueGameId;
	}

	/**
	 * Delete file object from S3 bucket. After creating the clips, the video
	 * is no longer necessary. As we'll be able to reference a game by its clips.
	 * 
	 * @param filename Key of stored item to remove
	 * @throws RuntimeException
	 */
	private void s3RemoveVideo(String filename) throws RuntimeException {
		DeleteObjectRequest req = DeleteObjectRequest.builder()
				.bucket(this.bucket)
				.key(filename)
				.build();

		CompletableFuture<DeleteObjectResponse> res = this.s3Client.deleteObject(req);
		res.whenComplete((deleteRes, ex) -> {
			if (deleteRes != null) {
				System.out.println("Deleted S3 object: " + filename);
			} else {
				throw new RuntimeException("S3 exception occurred during delete");
			}
		});
	}

	/**
	 * Delete file within the downloads directory.
	 * 
	 * @param filename Name of downloaded file to delete
	 */
	public void deleteDownload(String filename) {
		Path path = Paths.get(this.downloadsPathname, filename);

		try {
			Files.delete(path);
			System.out.println("File deleted: " + path.toString());
		} catch (NoSuchFileException e) {
			System.out.println("File not found: " + path.toString());
		} catch (Exception e) {
			System.out.println("Error deleting " + filename + ": " + e.getMessage());
		}
	}
}
