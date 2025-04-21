package com.hooparchives;

import java.io.IOException;
import java.nio.file.Paths;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

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

	public void trimVideo(String filename, String startTime, String duration) {
		String inputFilePath = this.downloadPath + "/" + filename;
		System.out.println("Input file path: " + inputFilePath);

		// perfrom trim request
		ProcessBuilder processBuilder = new ProcessBuilder(
				"ffmpeg",
				"-i", inputFilePath,
				"-ss", startTime,
				"-t", duration,
				"-c", "copy", // copy codecs (w/o re-encoding)
				Paths.get(this.clipOutputPath, filename).toString());

		try {
			Process process = processBuilder.start();
			int exitCode = process.waitFor();

			if (exitCode == 0) {
				System.out.println("✅ Video trimmed successfully!");
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

		// TODO: upload to s3
	}
}
