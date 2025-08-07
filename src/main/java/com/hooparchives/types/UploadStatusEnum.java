package com.hooparchives.types;

public enum UploadStatusEnum {
	PENDING("pending"),
	UPLOADING("uploading"),
	COMPLETE("complete"),
	FAILED("failed");

	public String status;

	UploadStatusEnum(String status) {
		this.status = status;
	}

	// custom string
	@Override
	public String toString() {
		return this.status;
	}
}