package com.hooparchives;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.hooparchives.TrimRequest.Clip;

import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

public class Clipper implements RequestHandler<TrimRequest, TrimResponse> {
	private final S3TransferManager s3TransferManager;
	private final S3AsyncClient s3Client;

	private final String region = System.getenv("AWS_REGION");
	private final String bucket = System.getenv("AWS_S3_BUCKET_NAME");

	private final String downloadsPathname = "/tmp/downloads";
	private final String clipsPathname = "/tmp/clips";

	private final String thumbnailFilenamePrefix = "THUMBNAIL_";

	public Clipper() {
		this.s3TransferManager = S3TransferManagerProvider.getTransferManager();
		this.s3Client = S3AsyncClientProvider.getClient();
	}

	/**
	 * Entry-point lambda function that downloads the targeted video from S3 and
	 * creates clips to be uploaded into S3. These clips are then created
	 * individually into the GameClips table and are grouped by the Games table.
	 * 
	 * @param req Trim request
	 * @return Trim response
	 * @throws Exception
	 * @throws IOException
	 */
	public TrimResponse handleRequest(TrimRequest req, Context context) {
		System.out.println("Entering trim request");
		ArrayList<String> clipUrls = new ArrayList<>();
		String thumbnailUrl = "";

		try {
			// ensure `/tmp/...` folders are created
			Files.createDirectories(Paths.get(this.downloadsPathname));
			Files.createDirectories(Paths.get(this.clipsPathname));

			// download video
			System.out.println("Downloading video");
			Path downloadsPath = Paths.get(this.downloadsPathname, req.key);
			this.downloadFile(downloadsPath, req.key);

			// process clips
			for (int i = 0; i < req.clips.size(); i++) {
				Clip clip = req.clips.get(i);
				Integer dotIndex = req.key.indexOf(".");
				String ext = req.key.substring(dotIndex);
				String clipName = req.key.substring(0, dotIndex) + "_" + i + ext;

				System.out.println("Processing " + clipName);

				Path clipOutputPath = Paths.get(this.clipsPathname, clipName);

				this.trimClip(downloadsPath, clipOutputPath, clip, clipName);
				String clipUrl = this.s3Upload(clipOutputPath, clipName);

				clipUrls.add(clipUrl);
			}

			// create thumbnail
			String baseName = req.key.replaceFirst("[.][^.]+$", ""); // remove extension
			String thumbnailFilename = this.thumbnailFilenamePrefix + baseName + ".jpg";
			Path thumbnailPath = createThumbnail(req.key, thumbnailFilename);
			thumbnailUrl = this.s3Upload(thumbnailPath, thumbnailFilename);

			// remove original video from downloads + s3 bucket
			System.out.println("Clips processed, cleaning up...");
			this.s3RemoveVideo(req.key);
			this.deleteDownload(req.key);
			this.deleteDownload(thumbnailFilename);
			this.deleteClips();

			System.out.println("Exiting trim request");
			return new TrimResponse(clipUrls, thumbnailUrl);
		} catch (Exception error) {
			context.getLogger().log(error.getMessage());
			return new TrimResponse(error.getMessage());
		}
	}

	/**
	 * Downloads a video from S3 and stores it into ~/tmp/downloads/*
	 * 
	 * @param destinationPath Downloads folder with the filename
	 * @param key             Key of stored item
	 */
	private void downloadFile(Path destinationPath, String key) throws Exception {
		DownloadFileRequest req = DownloadFileRequest.builder()
				.getObjectRequest(b -> b.bucket(this.bucket).key(key))
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
			throws InterruptedException, Exception {
		ProcessBuilder processBuilder = new ProcessBuilder(
				"ffmpeg",
				"-i", videoInputPath.toString(),
				"-ss", clip.start,
				"-t", clip.duration,
				"-c", "copy", // copy codecs (w/o re-encoding)
				clipOutputPath.toString());

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
	 * @param key      Key of item to store
	 * @return Clip url
	 * @throws Exception
	 */
	private String s3Upload(Path clipPath, String key) throws Exception {
		UploadFileRequest req = UploadFileRequest.builder()
				.putObjectRequest(b -> b.bucket(this.bucket).key(key))
				.source(clipPath)
				.build();

		FileUpload upload = this.s3TransferManager.uploadFile(req);
		CompletedFileUpload uploadResult = upload.completionFuture().join();
		SdkHttpResponse res = uploadResult.response().sdkHttpResponse();

		if (!res.isSuccessful()) {
			throw new Exception("Invalid S3 put request");
		}

		String url = "https://" + this.bucket + ".s3." + this.region + ".amazonaws.com/" + key;
		System.out.println("Successful put request: " + url);

		return url;
	}

	/**
	 * Delete file object from S3 bucket. After creating the clips, the video
	 * is no longer necessary. As we'll be able to reference a game by its clips.
	 * 
	 * @param key Key of stored item to remove
	 * @throws RuntimeException
	 */
	private void s3RemoveVideo(String key) throws RuntimeException {
		DeleteObjectRequest req = DeleteObjectRequest.builder()
				.bucket(this.bucket)
				.key(key)
				.build();

		this.s3Client.deleteObject(req).join(); // waits until delete is complete
		System.out.println("Deleted S3 object: " + key);
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

	public void deleteClips() {
		Path path = Paths.get(this.clipsPathname);

		try {
			if (Files.exists(path) && Files.isDirectory(path)) {
				Files.list(path)
						.forEach(file -> {
							try {
								Files.deleteIfExists(file);
								System.out.println("Deleted: " + file);
							} catch (IOException e) {
								System.out.println("Failed to delete: " + file + " - " + e.getMessage());
							}
						});
			}
		} catch (IOException e) {
			System.out.println("Error cleaning clip folder: " + e.getMessage());
		}
	}

	/**
	 * Create thumbnail from first frame of video
	 * 
	 * @param videoFilename     Video to source
	 * @param thumbnailFilename Name of thumbnail to create
	 * @return Path to thumbnail image
	 * @throws Exception
	 */
	public Path createThumbnail(String videoFilename, String thumbnailFilename) throws Exception {
		Path videoPath = Paths.get(this.downloadsPathname, videoFilename);
		Path thumbnailPath = Paths.get(this.downloadsPathname, thumbnailFilename);
		String startOfVideo = "00:00:01";

		ProcessBuilder processBuilder = new ProcessBuilder(
				"ffmpeg",
				"-ss", startOfVideo,
				"-i", videoPath.toString(),
				"-frames:v", "1",
				"-q:v", "2",
				thumbnailPath.toString());

		Process process = processBuilder.start();
		int exitCode = process.waitFor();

		if (exitCode != 0) {
			throw new Exception("Process failed with exit code: " + exitCode);
		}

		return thumbnailPath;
	}
}
