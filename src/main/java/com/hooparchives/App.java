package com.hooparchives;

import io.javalin.Javalin;
// import java.util.Map;

public class App {
	public static void main(String[] args) {
		Javalin app = Javalin.create(config -> {
			// config.defaultContentType = "application/json";
		}).start(7070);

		// Health check
		app.get("/", ctx -> ctx.result("Hello world!"));

		// Example /clip endpoint
		// app.post("/clip", ctx -> {
		// Map<String, String> params = ctx.bodyAsClass(Map.class);
		// String s3Url = params.get("s3Url");
		// String start = params.get("start");
		// String end = params.get("end");

		// ctx.json(Map.of(
		// "status", "processing started",
		// "s3Url", s3Url,
		// "start", start,
		// "end", end));
		// });

		app.get("/upload", ctx -> {
			// Extract the 'filename' query parameter from the request
			String filename = ctx.queryParam("filename");

			if (filename == null) {
				ctx.status(400).result("Filename is required.");
				return;
			}

			// Create a Clipper instance and call downloadFile with the filename
			Clipper clipper = new Clipper();
			clipper.downloadFile(filename);

			ctx.status(200).result("Download initiated for file: " + filename);
		});
	}
}
