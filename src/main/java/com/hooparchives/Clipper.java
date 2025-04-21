package com.hooparchives;

import java.nio.file.Paths;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

public class Clipper {
	private final S3TransferManager manager;
	private final String bucket = "hoop-archives-uploads";
	private final String downloadPath = "downloads";

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
}
