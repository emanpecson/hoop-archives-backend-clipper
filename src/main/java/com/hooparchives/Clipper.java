package com.hooparchives;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.amazonaws.services.lambda.runtime.Context;
import com.hooparchives.types.UploadRequest;
import com.hooparchives.types.ClipRequest;
import com.hooparchives.types.GameStatusEnum;

public class Clipper implements RequestHandler<SQSEvent, Void> {
	private final S3TransferManagerHandler s3TransferManager;
	// private final S3AsyncClientHandler s3AsyncClient;
	private final DdbHandler ddb;

	private final String downloadsPathname = "/tmp/downloads";
	private final String clipsPathname = "/tmp/clips";
	private final String thumbnailPrefix = "thumbnail_";
	private final String combinedClipsPrefix = "combined_";
	private final String ext = ".mp4";

	public Clipper() {
		this.s3TransferManager = new S3TransferManagerHandler();
		// this.s3AsyncClient = new S3AsyncClientHandler();
		this.ddb = new DdbHandler();
	}

	/**
	 * Entry-point lambda function that downloads the targeted video from S3 and
	 * creates clips to be uploaded into S3. These clips are then created
	 * individually into the GameClips table and are grouped by the Games table.
	 * 
	 * @param req Trim request
	 * @return Trim response
	 * @throws Exception
	 * @throws IOException
	 */
	@Override
	public Void handleRequest(SQSEvent event, Context context) {
		context.getLogger().log("[Clipper] Entering");

		for (SQSEvent.SQSMessage message : event.getRecords()) {
			String body = message.getBody();
			context.getLogger().log("[Clipper] Message: " + body);
			UploadRequest req = null;

			try {
				// parse body as the expected request type
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new Jdk8Module());
				req = mapper.readValue(body, UploadRequest.class);

				ddb.updateGameStatus(req, GameStatusEnum.UPLOADING);

				// ensure `/tmp/...` folders are created
				Files.createDirectories(Paths.get(this.downloadsPathname));
				Files.createDirectories(Paths.get(this.clipsPathname));

				// download game video by bucket key
				Path downloadsPath = Paths.get(this.downloadsPathname, req.key);
				context.getLogger().log("[Clipper] Downloading video: " + downloadsPath.toString());
				s3TransferManager.downloadFile(downloadsPath, req.key);

				// update game thumbnail
				String thumbnailFilename = this.thumbnailPrefix + req.gameId + ".jpg";
				Path thumbnailPath = createThumbnail(req.key, thumbnailFilename);
				String thumbnailUrl = s3TransferManager.upload(thumbnailPath, thumbnailFilename, context);
				ddb.updateGameThumbnail(req, thumbnailUrl);

				// process clips
				for (int i = 0; i < req.clipRequests.size(); i++) {
					ClipRequest clip = req.clipRequests.get(i);
					String log = String.format("[Clipper] Processing %s (%d/%d)", clip.clipId, i + 1, req.clipRequests.size());
					context.getLogger().log(log);

					// define clip download path
					String clipFilename = clip.clipId + this.ext;
					Path clipOutputPath = Paths.get(this.clipsPathname, clipFilename);

					// trim clip + put in s3
					this.trimClip(downloadsPath, clipOutputPath, clip);
					String clipUrl = s3TransferManager.upload(clipOutputPath, clipFilename, context);

					// create clip in ddb
					ddb.createGameClip(req, clip, clipUrl);
				}

				// combine all clips + upload
				context.getLogger().log("[Clipper] Combining all clips");
				String gameClipsKey = this.combinedClipsPrefix + req.gameId + this.ext;
				Path combinedOutputPath = combineClips(req, gameClipsKey, context);
				String gameClipsUrl = s3TransferManager.upload(combinedOutputPath, gameClipsKey, context);
				ddb.setGameClipsUrl(req, gameClipsUrl);

				// remove original video from downloads + s3 bucket
				context.getLogger().log("[Clipper] Cleaning up");
				this.deleteDownload(req.key, context);
				this.deleteDownload(thumbnailFilename, context);
				this.deleteClips(context);

				ddb.updateGameStatus(req, GameStatusEnum.COMPLETE);
			} catch (Exception error) {
				context.getLogger().log("[Clipper] " + error.getMessage());

				if (req != null) {
					try {
						ddb.updateGameStatus(req, GameStatusEnum.FAILED);
					} catch (Exception ddbError) {
						context.getLogger().log("[Clipper] " + error.getMessage());
					}
				}
			}
		}

		context.getLogger().log("[Clipper] Exiting");
		return null;
	}

	/**
	 * Starts a process with FFMPEG to trim a video into a clip.
	 * 
	 * @param videoInputPath Path to video to edit
	 * @param clipOutputPath Path to store created clips
	 * @param clip           Clip request
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws Exception
	 */
	private void trimClip(Path videoInputPath, Path clipOutputPath, ClipRequest clip)
			throws InterruptedException, Exception {
		Float duration = clip.endTime - clip.startTime;

		ProcessBuilder processBuilder = new ProcessBuilder(
				System.getenv("FFMPEG_PATH"),
				"-y",
				"-ss", clip.startTime.toString(),
				"-i", videoInputPath.toString(),
				"-t", duration.toString(),
				"-r", "30", // force frame rate
				"-pix_fmt", "yuv420p", // match pixel format
				"-ar", "44100", // standard audio sample rate
				"-ac", "2", // stereo
				"-c:v", "libx264", // encode as .mp4 compatible video
				"-c:a", "aac", // encode as .mp4 compatible audio
				"-preset", "fast",
				"-crf", "22", // match CRF
				"-b:a", "128k", // match audio bitrate
				clipOutputPath.toString());

		Process process = processBuilder.start();
		int exitCode = process.waitFor();

		if (exitCode != 0) {
			throw new Exception("Process failed with exit code: " + exitCode);
		}
	}

	/**
	 * Delete file within the downloads directory.
	 * 
	 * @param filename Name of downloaded file to delete
	 */
	private void deleteDownload(String filename, Context context) {
		Path path = Paths.get(this.downloadsPathname, filename);

		try {
			Files.delete(path);
			context.getLogger().log("[Clipper] File deleted: " + path.toString());
		} catch (NoSuchFileException e) {
			context.getLogger().log("[Clipper] File not found: " + path.toString());
		} catch (Exception e) {
			context.getLogger().log("[Clipper] Error deleting " + filename + ": " + e.getMessage());
		}
	}

	/**
	 * Delete all files within the clips folder
	 * 
	 * @param context
	 */
	private void deleteClips(Context context) {
		Path path = Paths.get(this.clipsPathname);

		try {
			if (Files.exists(path) && Files.isDirectory(path)) {
				Files.list(path)
						.forEach(file -> {
							try {
								Files.deleteIfExists(file);
								context.getLogger().log("[Clipper] Deleted: " + file);
							} catch (IOException e) {
								context.getLogger().log("[Clipper] Failed to delete: " + file + " - " + e.getMessage());
							}
						});
			}
		} catch (IOException e) {
			context.getLogger().log("[Clipper] Error cleaning clip folder: " + e.getMessage());
		}
	}

	/**
	 * Create thumbnail from first frame of video
	 * 
	 * @param videoFilename     Video to source
	 * @param thumbnailFilename Name of thumbnail to create
	 * @return Path to thumbnail image
	 * @throws Exception
	 */
	private Path createThumbnail(String videoFilename, String thumbnailFilename) throws Exception {
		Path videoPath = Paths.get(this.downloadsPathname, videoFilename);
		Path thumbnailPath = Paths.get(this.downloadsPathname, thumbnailFilename);
		String startOfVideo = "00:00:01";

		ProcessBuilder processBuilder = new ProcessBuilder(
				System.getenv("FFMPEG_PATH"),
				"-y",
				"-ss", startOfVideo,
				"-i", videoPath.toString(),
				"-frames:v", "1",
				"-q:v", "2",
				thumbnailPath.toString());

		Process process = processBuilder.start();
		int exitCode = process.waitFor();

		if (exitCode != 0) {
			throw new Exception("Process failed with exit code: " + exitCode);
		}

		return thumbnailPath;
	}

	/**
	 * To compile clips together, FFmpeg needs a list of how it should be
	 * sorted together.
	 * 
	 * Then run the concat FFmpeg concat command.
	 * 
	 * @param req
	 * @throws IOException
	 */
	private Path combineClips(UploadRequest req, String gameClipsKey, Context context)
			throws IOException, InterruptedException, Exception {

		Path clipsListFilePath = Paths.get(this.clipsPathname, "clips_list.txt");

		// copy clips into a clips_list.txt file for ffmpeg to process
		try (BufferedWriter writer = Files.newBufferedWriter(clipsListFilePath)) {

			for (int i = 0; i < req.clipRequests.size(); i++) {
				ClipRequest clip = req.clipRequests.get(i);
				String clipFilename = clip.clipId + this.ext;
				Path clipPath = Paths.get(this.clipsPathname, clipFilename);

				if (!Files.exists(clipPath)) {
					context.getLogger().log("[Clipper] Missing clip to concatenate: " + clipPath);
				}

				String concatLog = String.format("[Clipper] Concatenating %s (%d/%d)", clip.clipId, i + 1,
						req.clipRequests.size());
				context.getLogger().log(concatLog);

				// line format: file '/path/to/my-clip.mp4'
				writer.write("file '" + clipPath.toString() + "'");
				writer.newLine();
			}

		} catch (Exception error) {
			context.getLogger().log("[Clipper] " + error.getMessage());
		}

		File tmpDir = new File("/tmp");
		long usableSpace = tmpDir.getUsableSpace() / (1024 * 1024); // MB
		context.getLogger().log("Available /tmp space: " + usableSpace + " MB");

		Path combinedOutputPath = Paths.get(this.clipsPathname, gameClipsKey);
		ProcessBuilder processBuilder = new ProcessBuilder(
				System.getenv("FFMPEG_PATH"),
				"-y",
				"-f", "concat",
				"-safe", "0",
				"-i", clipsListFilePath.toString(),
				"-c", "copy", // copy both video and audio streams
				combinedOutputPath.toString());

		context.getLogger().log("[Clipper] FFmpeg is attempting to combine clips @ " + combinedOutputPath.toString());

		Process process = processBuilder.start();
		int exitCode = process.waitFor();

		if (exitCode != 0) {
			throw new Exception("Process failed with exit code: " + exitCode);
		}

		context.getLogger().log("[Clipper] FFmpeg has finished combining clips");
		return combinedOutputPath;
	}
}
