"""
Pure file/ffmpeg work — no transport, no S3, no Kafka.
Absorbs Generator.py + Burner.py + the snap_to_frame and load logic
that was inline in the old Worker.py.
"""
import os
import json
import subprocess

from app.Config.Config import config


class SubtitleProcessingService:

    @staticmethod
    def _snap_to_frame(seconds:float,fps: float)-> float:
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

    def snap_segments(
            self, segments: list[dict], fps: float, duration: float = 0
    )-> list[dict]:
        """Snap each segment's timestamps to frame boundaries."""
        snapped = []

        for seg in segments:
            text = (seg.get("text")or "").strip()
            if not text:
                continue
            start = self._snap_to_frame(float(seg.get("start",0.0)),fps)
            end = self._snap_to_frame(float(seg.get("end",duration)),fps)
            if end <= start:
                end = self._snap_to_frame(start + (1.0/fps),fps)
            snapped.append({"start":start, "end":end, "text":text})
        return snapped
    def merge_transcripts(self, segments:list[dict]) -> dict:
        full_text = "".join(s["text"] for s in segments if s.get("text"))
        return {
            "text": full_text.strip(),
            "segments": segments,
            "total_segments": len(segments),

        }
    def save_transcripts(self,transcript:dict,output_path:str)-> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path,"w",encoding="utf-8") as f:
            json.dump(transcript,f,ensure_ascii=False,indent=2)
        return output_path
    def generate_srt(self,segments:list[dict],output_path:str)-> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        lines = []
        for i,seg in enumerate(segments,1):
            text = seg.get("text").strip()
            if not text:
                continue
            start = self._format_srt(seg.get("start",0.0))
            end = self._format_srt(seg.get("end",0.0))
            lines.append(str(i))
            lines.append(f"{start} ---> {end}")
            lines.append(text)
            lines.append("")
        with open(output_path,"w",encoding="utf-8") as f:
            f.write("\n".join(lines))
        return output_path
    def generate_vtt(self,segments:list[dict],output_path:str)-> str:
        output = os.makedirs(os.path.dirname(output_path), exist_ok=True)
        lines = ["VTT",""]
        for seg in segments:
            text = seg.get("text","").strip()
            if not text:
                continue
            start = self._format_vtt(seg.get("start",0.0))
            end = self._format_vtt(seg.get("end",0.0))
            lines.append(f"{start} ---> {end}")
            lines.append(text)
            lines.append("")
        with open(output_path,"w",encoding="utf-8") as f:
            f.write("\n".join(lines))
        return output_path

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








