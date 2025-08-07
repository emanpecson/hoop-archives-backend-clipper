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
import com.hooparchives.types.ReelRequest;
import com.hooparchives.types.UploadStatusEnum;

public class ReelClipper extends Clipper implements RequestHandler<SQSEvent, Void> {

	/**
	 * Entry-point lambda function that downloads the targeted clips from S3 and
	 * compiles them into a single video.
	 */
	@Override
	public Void handleRequest(SQSEvent event, Context context) {
		context.getLogger().log("[Clipper] Entering");

		for (SQSEvent.SQSMessage message : event.getRecords()) {
			String body = message.getBody();
			context.getLogger().log("[Clipper] Message: " + body);
			ReelRequest req = null;

			try {
				// parse body as the expected request type
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new Jdk8Module());
				req = mapper.readValue(body, ReelRequest.class);

				ddb.setReelUploadStatus(req, UploadStatusEnum.UPLOADING);

				// ensure `/tmp/...` folders are created
				Files.createDirectories(Paths.get(this.clipsPathname));
				Files.createDirectories(Paths.get(this.downloadsPathname));

				// download all clips by bucket key
				List<String> clipFilenames = new ArrayList<>();
				for (int i = 0; i < req.clipKeys.size(); i++) {
					String clipKey = req.clipKeys.get(i);

					// download game video by bucket key
					Path clipsPath = Paths.get(this.clipsPathname, clipKey);
					String log = String.format("[Clipper] Downloading video: %s (%d/%d)", clipKey, i + 1, req.clipKeys.size());
					context.getLogger().log(log);
					s3TransferManager.downloadFile(clipsPath, clipKey);

					clipFilenames.add(clipKey); // clip key includes extension (.mp4)

					// set thumbnail
					if (i == 0) {
						String thumbnailFilename = this.thumbnailPrefix + req.reelId + ".jpg";

						// create thumbnail from first clip
						Path thumbnailPath = createThumbnail(this.clipsPathname, clipFilenames.get(0), downloadsPathname,
								thumbnailFilename);
						String thumbnailUrl = s3TransferManager.upload(thumbnailPath, thumbnailFilename, context);
						ddb.setReelThumbnailUrl(req, thumbnailUrl);
					}
				}

				// combine all clips + upload
				context.getLogger().log("[Clipper] Combining all clips");
				String key = this.combinedClipsPrefix + req.reelId + this.ext;
				Path combinedOutputPath = combineClips(clipFilenames, key, context);
				String sourceUrl = s3TransferManager.upload(combinedOutputPath, key, context);
				ddb.setReelSourceUrl(req, sourceUrl);

				ddb.setReelUploadStatus(req, UploadStatusEnum.COMPLETE);
			} catch (Exception error) {
				context.getLogger().log("[Clipper] " + error.getMessage());

				if (req != null) {
					try {
						ddb.setReelUploadStatus(req, UploadStatusEnum.FAILED);
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
