"""
Pure file/ffmpeg work — no transport, no S3, no Kafka.
Absorbs Generator.py + Burner.py + the snap_to_frame and load logic
that was inline in the old Worker.py.
"""
import os
import json
import subprocess

from app.Config.Config import config


# ── Tuning knobs ──────────────────────────────────────────────
MAX_SUBTITLE_DURATION = 7.0   # seconds — Arabic broadcast standard
MIN_SUBTITLE_DURATION = 1.0   # don't create slivers shorter than this
MIN_CHARS_PER_SEC     = 2.0   # below this = sparse text over silence, cap don't split


class SubtitleProcessingService:

    # ── low-level helpers ─────────────────────────────────────

    @staticmethod
    def _snap_to_frame(seconds: float, fps: float) -> float:
        frame = round(seconds * fps)
        return frame / fps

    @staticmethod
    def _format_srt(seconds: float) -> str:
        if seconds < 0:
            seconds = 0
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int(round((seconds - int(seconds)) * 1000))
        if millis >= 1000:
            millis = 0
            secs += 1
        return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

    @staticmethod
    def _format_vtt(seconds: float) -> str:
        if seconds < 0:
            seconds = 0
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int(round((seconds - int(seconds)) * 1000))
        if millis >= 1000:
            millis = 0
            secs += 1
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"

    # ── max-duration splitting ────────────────────────────────

    @staticmethod
    def _find_split_point(text: str) -> int | None:
        """
        Best character index to split Arabic text at.
        Priority: punctuation near midpoint > space near midpoint.
        Returns index of first char of the second half, or None.
        """
        mid = len(text) // 2
        best = None
        best_score = float("inf")

        # Pass 1: punctuation
        for i, ch in enumerate(text):
            if ch in "،.؟!؛:,?":
                pos = i + 1
                if pos < len(text):
                    score = abs(pos - mid)
                    if score < best_score:
                        best_score = score
                        best = pos
        if best is not None:
            return best

        # Pass 2: word boundary (space)
        best_score = float("inf")
        for i, ch in enumerate(text):
            if ch == " ":
                pos = i + 1
                if 0 < pos < len(text):
                    score = abs(pos - mid)
                    if score < best_score:
                        best_score = score
                        best = pos
        return best

    def _split_long_segment(
        self, seg: dict, fps: float, max_dur: float = MAX_SUBTITLE_DURATION,
    ) -> list[dict]:
        """
        Split a segment that exceeds max_dur.
        If text is too sparse for the duration (hallucination over silence),
        cap the display time instead of splitting into single-word subs.
        """
        start = float(seg["start"])
        end   = float(seg["end"])
        text  = seg["text"].strip()
        dur   = end - start

        # Short enough — keep as-is
        if dur <= max_dur or len(text) < 4:
            return [seg]

        # Sparse text over long silence? Cap, don't split.
        char_count = len(text.replace(" ", ""))
        chars_per_sec = char_count / dur
        if chars_per_sec < MIN_CHARS_PER_SEC:
            capped = max(MIN_SUBTITLE_DURATION, min(max_dur, char_count / 4.0))
            capped_end = self._snap_to_frame(start + capped, fps)
            return [{"start": start, "end": capped_end, "text": text}]

        # Find best split point
        split_idx = self._find_split_point(text)
        if split_idx is None:
            return [seg]

        left_text  = text[:split_idx].strip()
        right_text = text[split_idx:].strip()
        if not left_text or not right_text:
            return [seg]

        # Time proportional to character count
        ratio    = len(left_text) / len(text)
        mid_time = self._snap_to_frame(start + dur * ratio, fps)

        # Enforce minimum durations
        if mid_time - start < MIN_SUBTITLE_DURATION:
            mid_time = self._snap_to_frame(start + MIN_SUBTITLE_DURATION, fps)
        if end - mid_time < MIN_SUBTITLE_DURATION:
            mid_time = self._snap_to_frame(end - MIN_SUBTITLE_DURATION, fps)

        left_seg  = {"start": start, "end": mid_time, "text": left_text}
        right_seg = {"start": mid_time, "end": end, "text": right_text}

        # Recurse — either half might still be too long
        return (
            self._split_long_segment(left_seg, fps, max_dur)
            + self._split_long_segment(right_seg, fps, max_dur)
        )

    # ── snap + split pipeline ─────────────────────────────────

    def snap_segments(
        self, segments: list[dict], fps: float, duration: float = 0,
    ) -> list[dict]:
        """Snap timestamps to frame grid, then split over-long segments."""
        result = []
        for seg in segments:
            text = (seg.get("text") or "").strip()
            if not text:
                continue
            start = self._snap_to_frame(float(seg.get("start", 0.0)), fps)
            end   = self._snap_to_frame(float(seg.get("end", duration)), fps)
            if end <= start:
                end = self._snap_to_frame(start + (1.0 / fps), fps)

            snapped = {"start": start, "end": end, "text": text}
            result.extend(self._split_long_segment(snapped, fps))

        return result

    # ── transcript helpers ────────────────────────────────────

    def merge_transcripts(self, segments: list[dict]) -> dict:
        full_text = " ".join(s["text"] for s in segments if s.get("text"))
        return {
            "text": full_text.strip(),
            "segments": segments,
            "total_segments": len(segments),
        }

    def save_transcripts(self, transcript: dict, output_path: str) -> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(transcript, f, ensure_ascii=False, indent=2)
        return output_path

    # ── SRT / VTT generation ──────────────────────────────────

    def generate_srt(self, segments: list[dict], output_path: str) -> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        lines = []
        for i, seg in enumerate(segments, 1):
            text = seg.get("text", "").strip()
            if not text:
                continue
            start = self._format_srt(seg.get("start", 0.0))
            end   = self._format_srt(seg.get("end", 0.0))
            lines.append(str(i))
            lines.append(f"{start} --> {end}")
            lines.append(text)
            lines.append("")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return output_path

    def generate_vtt(self, segments: list[dict], output_path: str) -> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        lines = ["WEBVTT", ""]
        for seg in segments:
            text = seg.get("text", "").strip()
            if not text:
                continue
            start = self._format_vtt(seg.get("start", 0.0))
            end   = self._format_vtt(seg.get("end", 0.0))
            lines.append(f"{start} --> {end}")
            lines.append(text)
            lines.append("")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return output_path

    # ── burn subtitles onto video ─────────────────────────────

    def burn_subtitles(
        self, video_path: str, srt_path: str, output_path: str,
    ) -> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        escaped_srt = srt_path.replace("\\", "/").replace(":", "\\:")
        cmd = [
            config.FFMPEG_PATH,
            "-i", video_path,
            "-vf", f"subtitles='{escaped_srt}':force_style="
                   f"'FontSize={config.FONT_SIZE},"
                   f"PrimaryColour=&H00FFFFFF,"
                   f"OutlineColour=&H00000000,"
                   f"Outline=2,"
                   f"MarginV=30'",
            "-c:a", "copy",
            "-y",
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg subtitle burn failed: {result.stderr}")
        return output_path