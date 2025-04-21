package com.hooparchives;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.hooparchives.TrimRequest.Clip;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

public class Clipper {
	private final S3TransferManager manager;
	private final String bucket = "hoop-archives-uploads";
	private final String downloadPath = "downloads";
	private final String clipOutputPath = "clips";

	public Clipper() {
		this.manager = S3TransferManagerProvider.getTransferManager();
	}

	public Long downloadFile(String filenameKey) {
		DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
				.getObjectRequest(b -> b.bucket(bucket).key(filenameKey))
				.destination(Paths.get(downloadPath, filenameKey))
				.build();

		FileDownload downloadFile = manager.downloadFile(downloadFileRequest);

		CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
		System.out.println("Content length: " + downloadResult.response().contentLength());
		return downloadResult.response().contentLength();
	}

	public void trimVideo(TrimRequest req) {
		// String inputFilePath = this.downloadPath + "/" + filename;
		Path inputFilePath = Paths.get(this.downloadPath, req.filename); // all clips from the same video

		for (int i = 0; i < req.clips.size(); i++) {
			Clip clip = req.clips.get(i);
			Integer dotIndex = req.filename.indexOf(".");
			String clipName = req.filename.substring(0, dotIndex) + "_" + i + req.filename.substring(dotIndex);

			Path outputFilePath = Paths.get(this.clipOutputPath, clipName);
			System.out.println("Input file path: " + inputFilePath);
			System.out.println("clip path: " + outputFilePath);

			// perfrom trim request
			ProcessBuilder processBuilder = new ProcessBuilder(
					"ffmpeg",
					"-i", inputFilePath.toString(),
					"-ss", clip.start,
					"-t", clip.duration,
					"-c", "copy", // copy codecs (w/o re-encoding)
					outputFilePath.toString());

			try {
				System.out.println("Starting ffmpeg request...");
				Process process = processBuilder.start();
				int exitCode = process.waitFor();

				if (exitCode == 0) {
					System.out.println("✅ Video trimmed successfully!");

					System.out.println("Uploading clip to S3...");
					String uploadResponse = this.uploadClip(clipName);
					System.out.println("upload response: " + uploadResponse);
				} else {
					System.out.println("❌ Error trimming video. Exit code: " + exitCode);
				}
			} catch (IOException e) {
				System.out.println("❗ IOException occurred: " + e.getMessage());
				e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println("⛔ Interrupted while trimming video: " + e.getMessage());
				Thread.currentThread().interrupt(); // restore interrupt status
			}

			// ? upload to ddb (later?)
		}
	}

	private String uploadClip(String filename) {
		UploadFileRequest req = UploadFileRequest.builder()
				.putObjectRequest(b -> b.bucket(this.bucket).key(filename))
				.source(Paths.get(this.clipOutputPath, filename))
				.build();

		FileUpload upload = manager.uploadFile(req);

		CompletedFileUpload uploadResult = upload.completionFuture().join();
		return uploadResult.response().eTag();
	}

	// private void createClip() {

	// }
}
