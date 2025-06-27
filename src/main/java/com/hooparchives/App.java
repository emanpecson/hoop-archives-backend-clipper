package com.hooparchives;

import java.util.Map;

import io.javalin.Javalin;
// import java.util.Map;

public class App {
	public static void main(String[] args) {
		final String appUrl = "http://localhost:3000";

		Javalin app = Javalin.create(config -> {
			// config.defaultContentType = "application/json";
		}).before(ctx -> {
			ctx.header("Access-Control-Allow-Origin", appUrl);
			ctx.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
			ctx.header("Access-Control-Allow-Headers", "Content-Type");
			ctx.header("Access-Control-Allow-Credentials", "true");
		}).start(7070);

		// Health check
		app.get("/", ctx -> {
			System.out.println("Welcome hit");
			ctx.json(Map.of("data", "Hello world"));
		});

		app.post("/video-clipper", ctx -> {
			try {
				TrimRequest req = ctx.bodyAsClass(TrimRequest.class);
				Clipper clipper = new Clipper();
				TrimResponse res = clipper.handleTrimRequests(req);

				ctx.status(200).json(Map.of("data", res));
			} catch (Exception e) {
				ctx.status(500).json(Map.of("error", e.getMessage()));
			}
		});
	}
}
