# tarjama-subtitle

Subtitle worker for the **Tarjama** Arabic ASR platform. A Kafka consumer that turns transcripts into SRT/VTT subtitle files, optionally burns subtitles into the video, registers the outputs in storage, and publishes a completion event.

## Architecture

A worker, layered consistently with the rest of the system:

- **Consumer / entrypoint** — subscribes to the subtitle topic and dispatches to the service layer.
- **Services** — builds SRT/VTT from transcripts and handles optional burn-in.
- **Repositories** — wrap object storage and the Kafka producer.
- **Config** — Kafka, object storage, and environment wiring.

Part of a multi-service system — see the [platform overview](https://github.com/maleksabbah/tarjama-docker) for the full architecture, pipeline flow, and the other services.
