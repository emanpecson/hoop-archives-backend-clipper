package com.hooparchives;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.amazonaws.services.lambda.runtime.Context;
import com.hooparchives.types.GameRequest;
import com.hooparchives.types.ClipRequest;
import com.hooparchives.types.UploadStatusEnum;

public class GameClipper extends Clipper implements RequestHandler<SQSEvent, Void> {
	private final String combinedClipsPrefix = "combined_";

	/**
	 * Entry-point lambda function that downloads the targeted video from S3 and
	 * creates clips to be uploaded into S3. These clips are then created
	 * individually into the GameClips table and are grouped by the Games table.
	 */
	@Override
	public Void handleRequest(SQSEvent event, Context context) {
		context.getLogger().log("[Clipper] Entering");

		for (SQSEvent.SQSMessage message : event.getRecords()) {
			String body = message.getBody();
			context.getLogger().log("[Clipper] Message: " + body);
			GameRequest req = null;

			try {
				// parse body as the expected request type
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new Jdk8Module());
				req = mapper.readValue(body, GameRequest.class);

				ddb.setGameUploadStatus(req, UploadStatusEnum.UPLOADING);

				// ensure `/tmp/...` folders are created
				Files.createDirectories(Paths.get(this.downloadsPathname));
				Files.createDirectories(Paths.get(this.clipsPathname));

				// download game video by bucket key
				Path downloadsPath = Paths.get(this.downloadsPathname, req.key);
				context.getLogger().log("[Clipper] Downloading video: " + downloadsPath.toString());
				s3TransferManager.downloadFile(downloadsPath, req.key);

				// process clips
				List<String> clipFilenames = new ArrayList<>();
				for (int i = 0; i < req.clipRequests.size(); i++) {
					ClipRequest clip = req.clipRequests.get(i);
					String log = String.format("[Clipper] Processing %s (%d/%d)", clip.clipId, i + 1, req.clipRequests.size());
					context.getLogger().log(log);

					// define clip download path
					String clipFilename = clip.clipId + this.ext;
					clipFilenames.add(clipFilename);

					// trim clip + put in s3
					Path clipOutputPath = Paths.get(this.clipsPathname, clipFilename);
					this.trimClip(downloadsPath, clipOutputPath, clip);
					String clipUrl = s3TransferManager.upload(clipOutputPath, clipFilename, context);

					if (i == 0) {
						// update game thumbnail
						String thumbnailFilename = this.thumbnailPrefix + req.gameId + ".jpg";
						Path thumbnailPath = createThumbnail(this.clipsPathname, clipFilename, this.downloadsPathname,
								thumbnailFilename);
						String thumbnailUrl = s3TransferManager.upload(thumbnailPath, thumbnailFilename, context);
						ddb.setGameThumbnailUrl(req, thumbnailUrl);
					}

					// create clip in ddb
					ddb.createGameClip(req, clip, clipUrl, clipFilename);
				}

				// combine all clips + upload
				context.getLogger().log("[Clipper] Combining all clips");
				String key = this.combinedClipsPrefix + req.gameId + this.ext;
				Path combinedOutputPath = combineClips(clipFilenames, key, context);
				String sourceUrl = s3TransferManager.upload(combinedOutputPath, key, context);
				ddb.setGameSourceUrl(req, sourceUrl);

				ddb.setGameUploadStatus(req, UploadStatusEnum.COMPLETE);
			} catch (Exception error) {
				context.getLogger().log("[Clipper] " + error.getMessage());

				if (req != null) {
					try {
						ddb.setGameUploadStatus(req, UploadStatusEnum.FAILED);
					} catch (Exception ddbError) {
						context.getLogger().log("[Clipper] " + error.getMessage());
					}
				}
			} finally {
				// remove video from paths
				context.getLogger().log("[Clipper] Cleaning up");
				this.deleteFiles(context, Paths.get(this.clipsPathname));
				this.deleteFiles(context, Paths.get(this.downloadsPathname));
			}
		}

		context.getLogger().log("[Clipper] Exiting");
		return null;
	}
}
