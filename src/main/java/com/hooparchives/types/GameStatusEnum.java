package com.hooparchives.types;

public enum GameStatusEnum {
	PENDING("pending"),
	UPLOADING("uploading"),
	COMPLETE("complete"),
	FAILED("failed");

	public String status;

	GameStatusEnum(String status) {
		this.status = status;
	}

	// custom string
	@Override
	public String toString() {
		return this.status;
	}
}