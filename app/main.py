"""
ASR Subtitle Service
=====================
Merges chunk transcriptions into SRT/VTT files and optionally burns onto video.
Runs as a background worker — no HTTP server.

Run:
  python -m app.main
"""
import asyncio
from app import Redis_client as rc
from app.Worker import process_task


async def main():
    print("Starting Subtitle Service...")
    await rc.init_redis()
    print("  Redis connected")
    print("Subtitle Service ready. Waiting for tasks...")

    try:
        while True:
            try:
                message = await rc.pop_subtitle_task(timeout=5)
                if message:
                    print(f"  [SUBTITLE] Received task for job {message.get('job_id')}")
                    await process_task(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"  [SUBTITLE] Error: {e}")
                await asyncio.sleep(1)
    finally:
        print("Shutting down Subtitle Service...")
        await rc.close_redis()
        print("Subtitle Service stopped.")


if __name__ == "__main__":
    asyncio.run(main())