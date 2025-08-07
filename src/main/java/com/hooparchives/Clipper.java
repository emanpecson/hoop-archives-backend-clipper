package com.hooparchives;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.hooparchives.types.ClipRequest;

public class Clipper {
	protected final S3TransferManagerHandler s3TransferManager;
	// protected final S3AsyncClientHandler s3AsyncClient;
	protected final DdbHandler ddb;

	protected final String downloadsPathname = "/tmp/downloads";
	protected final String clipsPathname = "/tmp/clips";
	protected final String thumbnailPrefix = "thumbnail_";
	protected final String combinedClipsPrefix = "combined_";
	protected final String ext = ".mp4";

	public Clipper() {
		this.s3TransferManager = new S3TransferManagerHandler();
		// this.s3AsyncClient = new S3AsyncClientHandler();
		this.ddb = new DdbHandler();
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
	protected void trimClip(Path videoInputPath, Path clipOutputPath, ClipRequest clip)
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
	 * Delete all files within the clips folder
	 * 
	 * @param context
	 */
	protected void deleteFiles(Context context, Path path) {
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
	protected Path createThumbnail(String videoFilename, String thumbnailFilename) throws Exception {
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
	protected Path combineClips(List<String> clipFilenames, String key, Context context)
			throws IOException, InterruptedException, Exception {

		Path clipsListFilePath = Paths.get(this.clipsPathname, "clips_list.txt");

		// copy clips into a clips_list.txt file for ffmpeg to process
		try (BufferedWriter writer = Files.newBufferedWriter(clipsListFilePath)) {

			for (int i = 0; i < clipFilenames.size(); i++) {
				String fn = clipFilenames.get(i);
				Path clipPath = Paths.get(this.clipsPathname, fn);

				if (!Files.exists(clipPath)) {
					context.getLogger().log("[Clipper] Missing clip to concatenate: " + clipPath);
				}

				String concatLog = String.format("[Clipper] Concatenating %s (%d/%d)", fn, i + 1,
						clipFilenames.size());
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

		Path combinedOutputPath = Paths.get(this.clipsPathname, key);
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
