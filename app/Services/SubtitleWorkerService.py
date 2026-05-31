# subtitle: app/Services/SubtitleWorkerService.py
"""
Subtitle worker orchestrator.
Consumes Kafka, processes one task end-to-end (load transcript → snap →
generate SRT/VTT → upload → register → publish completion; optional burn
publishes a second completion), commits the Kafka offset on success.
"""
import os
import tempfile

from app.Config.Config import config
from app.Repositories import (
    EventConsumer,
    EventPublisher,
    S3Client,
    StorageClient,
)
from app.Services.SubtitleProcessingService import SubtitleProcessingService


class SubtitleWorkerService:
    def __init__(
        self,
        consumer: EventConsumer,
        publisher: EventPublisher,
        s3: S3Client,
        storage: StorageClient,
        processing: SubtitleProcessingService,
    ):
        self.consumer = consumer
        self.publisher = publisher
        self.s3 = s3
        self.storage = storage
        self.processing = processing

    async def run(self) -> None:
        await self.consumer.start()
        print("  [SUBTITLE] Consumer started")
        try:
            async for message in self.consumer.messages():
                try:
                    await self.process(message)
                    await self.consumer.commit()
                except Exception as e:
                    # Don't commit — Kafka redelivers.
                    print(f"  [SUBTITLE] Handler error, will redeliver: {e}")
        finally:
            await self.consumer.stop()
            print("  [SUBTITLE] Consumer stopped")

    async def process(self, message: dict) -> None:
        task_id = message["task_id"]
        job_id = message["job_id"]
        user_id = message.get("user_id", 0)
        original_video = message["original_video"]
        subtitle_format = message.get("format", "srt")
        burn = message.get("burn", False)

        print(f"  [SUBTITLE] Processing job {job_id} (burn={burn})")

        with tempfile.TemporaryDirectory() as tmp_dir:
            # 1. Load video meta for fps (with fallback)
            try:
                meta = self.s3.download_json(f"audio/{job_id}/video_meta.json")
                fps = float(meta.get("fps", config.DEFAULT_FPS))
                duration = float(meta.get("duration", 0))
            except Exception as e:
                print(f"  [SUBTITLE] video_meta.json missing, using default fps: {e}")
                fps = config.DEFAULT_FPS
                duration = 0
            print(f"  [SUBTITLE] fps={fps:.3f}")

            # 2. Load transcript
            transcript_data = self.s3.download_json(f"results/{job_id}/transcript.json")
            raw_segments = transcript_data.get("segments", [])
            if not raw_segments and transcript_data.get("text"):
                raw_segments = [{
                    "start": 0.0,
                    "end": transcript_data.get("duration_seconds", duration),
                    "text": transcript_data["text"],
                }]

            segments = self.processing.snap_segments(
                raw_segments, fps, duration=duration,
            )
            transcript = self.processing.merge_transcripts(segments)
            print(f"  [SUBTITLE] {len(segments)} segments")

            # 3. Save and upload transcript
            local_transcript = os.path.join(tmp_dir, "transcript.json")
            self.processing.save_transcripts(transcript, local_transcript)
            transcript_key = f"results/{job_id}/transcript.json"
            self.s3.upload_file(local_transcript, transcript_key)
            await self.storage.register_file(
                job_id=job_id, user_id=user_id,
                category="transcript", file_type="json",
                path=transcript_key, mime_type="application/json",
            )
            outputs = {"transcript": transcript_key}

            # 4. Generate subtitle files
            if subtitle_format in ("srt", "both"):
                local_srt = os.path.join(tmp_dir, "subtitles.srt")
                self.processing.generate_srt(segments, local_srt)
                srt_key = f"results/{job_id}/subtitles.srt"
                self.s3.upload_file(local_srt, srt_key)
                outputs["srt"] = srt_key
                await self.storage.register_file(
                    job_id=job_id, user_id=user_id,
                    category="subtitle", file_type="srt",
                    path=srt_key, mime_type="application/x-subrip",
                )
                print("  [SUBTITLE] SRT done")

            if subtitle_format in ("vtt", "both"):
                local_vtt = os.path.join(tmp_dir, "subtitles.vtt")
                self.processing.generate_vtt(segments, local_vtt)
                vtt_key = f"results/{job_id}/subtitles.vtt"
                self.s3.upload_file(local_vtt, vtt_key)
                outputs["vtt"] = vtt_key
                await self.storage.register_file(
                    job_id=job_id, user_id=user_id,
                    category="subtitle", file_type="vtt",
                    path=vtt_key, mime_type="text/vtt",
                )
                print("  [SUBTITLE] VTT done")

            # 5. First completion (subtitles stage)
            await self.publisher.publish_completion({
                "task_id": task_id,
                "job_id": job_id,
                "type": "subtitle",
                "stage": "subtitles",
                "status": "completed",
                "final": not burn,
                "outputs": outputs,
            })
            print(f"  [SUBTITLE] Published subtitles-stage for {job_id}")

            # 6. Burn (if requested)
            if burn:
                local_video = os.path.join(tmp_dir, "video.mp4")
                self.s3.download_file(original_video, local_video)

                local_srt_for_burn = os.path.join(tmp_dir, "subtitles.srt")
                if not os.path.exists(local_srt_for_burn):
                    self.processing.generate_srt(segments, local_srt_for_burn)

                local_burned = os.path.join(tmp_dir, "video_subtitled.mp4")
                self.processing.burn_subtitles(
                    local_video, local_srt_for_burn, local_burned,
                )

                video_key = f"results/{job_id}/video_subtitled.mp4"
                self.s3.upload_file(local_burned, video_key)
                await self.storage.register_file(
                    job_id=job_id, user_id=user_id,
                    category="video", file_type="mp4",
                    path=video_key, mime_type="video/mp4",
                )

                # 7. Second completion (burn stage)
                await self.publisher.publish_completion({
                    "task_id": task_id,
                    "job_id": job_id,
                    "type": "subtitle",
                    "stage": "burn",
                    "status": "completed",
                    "final": True,
                    "outputs": {"video": video_key},
                })
                print(f"  [SUBTITLE] Published burn-stage for {job_id}")

        print(f"  [SUBTITLE] Job {job_id} done")