package com.hooparchives;

import java.util.ArrayList;

import io.javalin.Javalin;
// import java.util.Map;

public class App {
	public static void main(String[] args) {
		Javalin app = Javalin.create(config -> {
			// config.defaultContentType = "application/json";
		}).start(7070);

		// Health check
		app.get("/", ctx -> {
			System.out.println("Welcome hit");
			ctx.result("Hello world!");
		});

		app.post("/trim", ctx -> {
			try {
				TrimRequest req = ctx.bodyAsClass(TrimRequest.class);

				Clipper clipper = new Clipper();
				ArrayList<String> clipUrls = clipper.handleTrimRequests(req);

				ctx.status(200).json(clipUrls);
			} catch (Exception e) {
				ctx.status(500).result("Error: " + e.getMessage());
			}
		});
	}
}
