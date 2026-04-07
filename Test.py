"""
Subtitle Service Unit Tests
==============================
Tests generator, burner, worker, and Redis client.

Run:
  pytest Test.py -v
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import json

# Explicit imports for patch
import app.Generator
import app.Burner
import app.Worker
import app.Redis_client


# =============================================================================
# Generator tests
# =============================================================================

class TestFormatTimestamps:
    def test_srt_format(self):
        from app.Generator import format_timestamp_srt
        assert format_timestamp_srt(0.0) == "00:00:00,000"
        assert format_timestamp_srt(3661.5) == "01:01:01,500"
        assert format_timestamp_srt(125.43) == "00:02:05,430"

    def test_vtt_format(self):
        from app.Generator import format_timestamp_vtt
        assert format_timestamp_vtt(0.0) == "00:00:00.000"
        assert format_timestamp_vtt(3661.5) == "01:01:01.500"
        assert format_timestamp_vtt(125.43) == "00:02:05.430"


class TestMergeTranscript:
    def test_merges_segments(self):
        from app.Generator import merge_transcript
        segments = [
            {"start": 0.0, "end": 1.5, "text": "كيف حالك"},
            {"start": 1.5, "end": 3.0, "text": "يا حبيبي"},
            {"start": 3.0, "end": 5.0, "text": "شو عم تعمل"},
        ]
        result = merge_transcript(segments)
        assert result["text"] == "كيف حالك يا حبيبي شو عم تعمل"
        assert result["total_segments"] == 3

    def test_handles_empty_segments(self):
        from app.Generator import merge_transcript
        result = merge_transcript([])
        assert result["text"] == ""
        assert result["total_segments"] == 0


class TestLoadChunkResults:
    def test_loads_and_merges_chunks(self):
        chunk_data = [
            {"segments": [{"start": 0.0, "end": 1.5, "text": "كيف حالك"}]},
            {"segments": [{"start": 30.0, "end": 32.0, "text": "شو عم تعمل"}]},
        ]

        mock_files = ["results/j_123/chunk_0000.json", "results/j_123/chunk_0001.json"]

        def mock_open_func(path, *args, **kwargs):
            idx = 0 if "0000" in path else 1
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)
            m.read = MagicMock(return_value=json.dumps(chunk_data[idx]))
            return m

        with patch("glob.glob", return_value=mock_files), \
             patch("builtins.open", side_effect=mock_open_func), \
             patch("json.load", side_effect=chunk_data):
            from app.Generator import load_chunk_results
            segments = load_chunk_results("results/j_123/")
            assert len(segments) == 2

    def test_raises_on_no_chunks(self):
        with patch("glob.glob", return_value=[]):
            from app.Generator import load_chunk_results
            with pytest.raises(RuntimeError, match="No chunk results found"):
                load_chunk_results("results/j_999/")


class TestGenerateSRT:
    def test_generates_srt_content(self):
        segments = [
            {"start": 0.0, "end": 1.5, "text": "كيف حالك"},
            {"start": 1.5, "end": 3.0, "text": "يا حبيبي"},
        ]

        written_content = []

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_file.write = MagicMock(side_effect=lambda x: written_content.append(x))

        with patch("builtins.open", return_value=mock_file), \
             patch("os.makedirs"):
            from app.Generator import generate_srt
            path = generate_srt(segments, "output/subtitles.srt")
            assert path == "output/subtitles.srt"

            content = written_content[0]
            assert "1" in content
            assert "00:00:00,000 --> 00:00:01,500" in content
            assert "كيف حالك" in content
            assert "2" in content
            assert "يا حبيبي" in content


class TestGenerateVTT:
    def test_generates_vtt_content(self):
        segments = [
            {"start": 0.0, "end": 1.5, "text": "كيف حالك"},
        ]

        written_content = []

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_file.write = MagicMock(side_effect=lambda x: written_content.append(x))

        with patch("builtins.open", return_value=mock_file), \
             patch("os.makedirs"):
            from app.Generator import generate_vtt
            path = generate_vtt(segments, "output/subtitles.vtt")
            assert path == "output/subtitles.vtt"

            content = written_content[0]
            assert "WEBVTT" in content
            assert "00:00:00.000 --> 00:00:01.500" in content
            assert "كيف حالك" in content


# =============================================================================
# Burner tests
# =============================================================================

class TestBurnSubtitles:
    def test_calls_ffmpeg(self):
        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result), \
             patch("os.makedirs"):
            from app.Burner import burn_subtitles
            path = burn_subtitles("video.mp4", "subtitles.srt", "output.mp4")
            assert path == "output.mp4"

    def test_raises_on_failure(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Invalid subtitle file"

        with patch("subprocess.run", return_value=mock_result), \
             patch("os.makedirs"):
            from app.Burner import burn_subtitles
            with pytest.raises(RuntimeError, match="FFmpeg subtitle burn failed"):
                burn_subtitles("video.mp4", "bad.srt", "output.mp4")


# =============================================================================
# Worker tests
# =============================================================================

@pytest.mark.asyncio
class TestSubtitleWorker:
    async def test_successful_srt_only(self):
        mock_push = AsyncMock()
        segments = [
            {"start": 0.0, "end": 1.5, "text": "كيف حالك"},
            {"start": 1.5, "end": 3.0, "text": "يا حبيبي"},
        ]
        transcript = {"text": "كيف حالك يا حبيبي", "segments": segments, "total_segments": 2}

        with patch("app.Worker.load_chunk_results", return_value=segments), \
             patch("app.Worker.merge_transcript", return_value=transcript), \
             patch("app.Worker.save_transcript", return_value="results/j_123/transcript.json"), \
             patch("app.Worker.generate_srt", return_value="results/j_123/subtitles.srt"), \
             patch("app.Worker.rc.push_completed", mock_push):
            from app.Worker import process_task
            await process_task({
                "task_id": "t_200",
                "job_id": "j_123",
                "results_dir": "results/j_123/",
                "original_video": "uploads/j_123/video.mp4",
                "format": "srt",
                "burn": False,
            })

            mock_push.assert_called_once()
            call_args = mock_push.call_args[0][0]
            assert call_args["status"] == "completed"
            assert call_args["type"] == "subtitle"
            assert "srt" in call_args["outputs"]
            assert "transcript" in call_args["outputs"]

    async def test_successful_with_burn(self):
        mock_push = AsyncMock()
        segments = [{"start": 0.0, "end": 1.5, "text": "كيف حالك"}]
        transcript = {"text": "كيف حالك", "segments": segments, "total_segments": 1}

        with patch("app.Worker.load_chunk_results", return_value=segments), \
             patch("app.Worker.merge_transcript", return_value=transcript), \
             patch("app.Worker.save_transcript", return_value="results/j_123/transcript.json"), \
             patch("app.Worker.generate_srt", return_value="results/j_123/subtitles.srt"), \
             patch("app.Worker.burn_subtitles", return_value="results/j_123/video_subtitled.mp4"), \
             patch("app.Worker.rc.push_completed", mock_push):
            from app.Worker import process_task
            await process_task({
                "task_id": "t_200",
                "job_id": "j_123",
                "results_dir": "results/j_123/",
                "original_video": "uploads/j_123/video.mp4",
                "format": "srt",
                "burn": True,
            })

            call_args = mock_push.call_args[0][0]
            assert call_args["status"] == "completed"
            assert "video" in call_args["outputs"]

    async def test_both_formats(self):
        mock_push = AsyncMock()
        segments = [{"start": 0.0, "end": 1.5, "text": "test"}]
        transcript = {"text": "test", "segments": segments, "total_segments": 1}

        with patch("app.Worker.load_chunk_results", return_value=segments), \
             patch("app.Worker.merge_transcript", return_value=transcript), \
             patch("app.Worker.save_transcript", return_value="transcript.json"), \
             patch("app.Worker.generate_srt", return_value="subtitles.srt"), \
             patch("app.Worker.generate_vtt", return_value="subtitles.vtt"), \
             patch("app.Worker.rc.push_completed", mock_push):
            from app.Worker import process_task
            await process_task({
                "task_id": "t_200",
                "job_id": "j_123",
                "results_dir": "results/j_123/",
                "original_video": "video.mp4",
                "format": "both",
                "burn": False,
            })

            call_args = mock_push.call_args[0][0]
            assert "srt" in call_args["outputs"]
            assert "vtt" in call_args["outputs"]

    async def test_failure_pushes_error(self):
        mock_push = AsyncMock()

        with patch("app.Worker.load_chunk_results", side_effect=RuntimeError("No chunks")), \
             patch("app.Worker.rc.push_completed", mock_push):
            from app.Worker import process_task
            await process_task({
                "task_id": "t_200",
                "job_id": "j_123",
                "results_dir": "results/j_123/",
                "original_video": "video.mp4",
            })

            call_args = mock_push.call_args[0][0]
            assert call_args["status"] == "failed"
            assert "No chunks" in call_args["error"]


# =============================================================================
# Redis client tests
# =============================================================================

@pytest.mark.asyncio
class TestRedisClient:
    async def test_pop_subtitle_task(self):
        mock_client = AsyncMock()
        mock_client.brpop.return_value = (
            "queue:subtitle",
            json.dumps({"task_id": "t_200", "job_id": "j_123"})
        )

        with patch("app.Redis_client.client", mock_client):
            from app.Redis_client import pop_subtitle_task
            result = await pop_subtitle_task()
            assert result["task_id"] == "t_200"

    async def test_pop_returns_none_on_timeout(self):
        mock_client = AsyncMock()
        mock_client.brpop.return_value = None

        with patch("app.Redis_client.client", mock_client):
            from app.Redis_client import pop_subtitle_task
            assert await pop_subtitle_task() is None

    async def test_push_completed(self):
        mock_client = AsyncMock()

        with patch("app.Redis_client.client", mock_client):
            from app.Redis_client import push_completed
            await push_completed({"task_id": "t_200", "status": "completed"})
            mock_client.lpush.assert_called_once()