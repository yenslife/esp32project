# Mac 本機監控伺服器（完整 MVP：即時無卡頓推播 + 畫面/聲音雙向動態偵測 + 前後 5 秒事件錄影 + 7 天清理 + Web Dashboard）
from array import array
import asyncio
from contextlib import asynccontextmanager
import hmac
from dataclasses import dataclass, field
from datetime import datetime
import io
import json
import math
import os
import re
import shutil
import struct
import subprocess
import tempfile
import time
import wave
import zlib
from pathlib import Path
from typing import Literal, get_args

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image, ImageChops, ImageFilter
from pydantic import BaseModel, Field, ValidationError

TCP_HOST = os.environ.get("ESP32_TCP_HOST", "0.0.0.0")
TCP_PORT = 9000
INGEST_PROTOCOL = "esp32-ingest-v1"
MAX_INGEST_PACKET = 128 * 1024
SAMPLE_RATE = 16000

CHUNK_US = 5_000_000          # 5 秒切塊：平時僅保留最新 1 塊作 Pre-roll
POST_ROLL_CHUNKS = 1          # 動靜結束後多保留 1 塊（5 秒 Post-roll）
MAX_EVENT_CHUNKS = 12         # 單支事件最長 60 秒（12 塊）

MOTION_DIFF_PX = 20           # 灰階亮度差門檻（0–255）
DEFAULT_MOTION_PCT = 2.5      # 預設畫面變動面積門檻（%）
MOTION_MIN_FRAMES = 2         # 5 秒內至少 2 張超標才觸發

AUDIO_REF_RMS = 2000.0        # 依 XIAO PDM 麥克風實測校準（安靜底噪約 0.8%，說話/拍手約 10–80%）
DEFAULT_AUDIO_PCT = 3.5       # 預設聲音變化門檻（%）
AUDIO_MIN_CHUNKS = 2          # 持續聲音至少 2 個 chunk（~256ms），或單次拍手 >= 2 倍門檻

TriggerMode = Literal["video", "audio", "either", "both", "continuous"]
CONFIG_PATH = Path("config.json")

REC_AUDIO = 1
REC_FRAME = 2
REC_STAT = 3
MAGIC = b"\xA5\x5A"
PKT_HDR = struct.Struct("<2sBIqI")  # magic(2s), type(u8), seq(u32), t_us(i64), len(u32)
SAFE_EVENT_ID = re.compile(r"^evt_\d{8}_\d{6}_\d{4}$")

RECORDINGS_ROOT = Path("recordings")
EVENTS_DIR = RECORDINGS_ROOT / "events"
STATIC_DIR = Path("static")
FFMPEG = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
DIFF_LUT = [255 if i > MOTION_DIFF_PX else 0 for i in range(256)]


class MotionDetector:
    """80x60 灰階模糊相鄰影格差分（純 C 運算，<0.1ms/張）。"""

    def __init__(self):
        self.prev_gray: Image.Image | None = None

    def reset(self):
        self.prev_gray = None

    def score(self, jpeg_bytes: bytes) -> float:
        gray = (
            Image.open(io.BytesIO(jpeg_bytes))
            .convert("L")
            .resize((80, 60), Image.Resampling.BILINEAR)
            .filter(ImageFilter.BoxBlur(1))
        )
        if self.prev_gray is None:
            self.prev_gray = gray
            return 0.0
        diff = ImageChops.difference(self.prev_gray, gray).point(DIFF_LUT)
        self.prev_gray = gray
        changed = diff.histogram()[255]
        return round(changed / (80 * 60) * 100.0, 2)


def calc_audio_level_pct(pcm_bytes: bytes) -> float:
    """扣除 PDM 麥克風直流偏移後計算 AC RMS，換算為 0–100% 聲音變化指標。"""
    n = len(pcm_bytes) // 2
    if n == 0:
        return 0.0
    samples = array("h")
    samples.frombytes(pcm_bytes[: n * 2])
    if os.sys.byteorder != "little":
        samples.byteswap()
    mean = sum(samples) / n
    ac_rms = math.sqrt(sum((x - mean) * (x - mean) for x in samples) / n)
    return round(min(100.0, ac_rms / AUDIO_REF_RMS * 100.0), 2)


@dataclass
class MediaChunk:
    start_us: int
    end_us: int
    wall_time: datetime
    frames: list[tuple[int, bytes, float]]
    audio: list[tuple[int, bytes, float]]
    motion_frames: int
    peak_motion_pct: float
    peak_jpeg: bytes
    audio_loud_chunks: int
    peak_audio_pct: float
    video_triggered: bool
    audio_triggered: bool


@dataclass
class ActiveEvent:
    started_at: datetime
    chunks: list[MediaChunk] = field(default_factory=list)
    post_roll_left: int = POST_ROLL_CHUNKS
    peak_motion_pct: float = 0.0
    peak_audio_pct: float = 0.0
    peak_jpeg: bytes = b""
    by_video: bool = False
    by_audio: bool = False
    continuous: bool = False


def mux_event_sync(stem_path: Path, started_at: datetime, chunks: list[MediaChunk],
                   next_frame_us: int | None, peak_motion_pct: float, peak_audio_pct: float,
                   trigger_reason: str, peak_jpeg: bytes) -> dict:
    """在背景執行緒將事件的所有 chunks（含 Pre-roll / Post-roll）轉為單支 H.264+AAC MP4。"""
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    frames = [(t, jpg) for c in chunks for (t, jpg, _) in c.frames]
    audio = [(t, pcm) for c in chunks for (t, pcm, _) in c.audio]

    mp4_path = stem_path.with_suffix(".mp4")
    jpg_path = stem_path.with_suffix(".jpg")
    json_path = stem_path.with_suffix(".json")

    with tempfile.TemporaryDirectory(prefix="esp32evt_") as tmp:
        tmp_dir = Path(tmp)
        pcm = b"".join(p for _, p in audio)
        wav_path = tmp_dir / "audio.wav"
        with wave.open(str(wav_path), "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(SAMPLE_RATE)
            wav.writeframes(pcm)

        for i, (_, jpg) in enumerate(frames):
            (tmp_dir / f"f{i:04d}.jpg").write_bytes(jpg)

        durations = []
        for i in range(len(frames)):
            if i + 1 < len(frames):
                d = max((frames[i + 1][0] - frames[i][0]) / 1e6, 0.005)
            elif next_frame_us is not None and next_frame_us > frames[i][0]:
                d = max((next_frame_us - frames[i][0]) / 1e6, 0.005)
            else:
                d = durations[-1] if durations else 0.125
            durations.append(d)

        concat_path = tmp_dir / "concat.txt"
        with concat_path.open("w") as f:
            for i, d in enumerate(durations):
                f.write(f"file 'f{i:04d}.jpg'\n")
                f.write(f"duration {d:.6f}\n")
            f.write(f"file 'f{len(frames) - 1:04d}.jpg'\n")

        audio_start_us = audio[0][0]
        video_start_us = frames[0][0]
        offset_s = (video_start_us - audio_start_us) / 1e6

        tmp_mp4 = stem_path.with_suffix(".tmp.mp4")
        cmd = [FFMPEG, "-y"]
        if offset_s >= 0:
            cmd += ["-itsoffset", f"{offset_s:.6f}",
                    "-f", "concat", "-safe", "0", "-i", str(concat_path),
                    "-i", str(wav_path)]
        else:
            cmd += ["-f", "concat", "-safe", "0", "-i", str(concat_path),
                    "-itsoffset", f"{-offset_s:.6f}", "-i", str(wav_path)]
        cmd += [
            "-fps_mode", "vfr",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-pix_fmt", "yuv420p",
            "-af", "highpass=f=80,volume=2.5",
            "-c:a", "aac", "-b:a", "64k",
            "-movflags", "+faststart",
            str(tmp_mp4),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        os.replace(tmp_mp4, mp4_path)

    if peak_jpeg:
        jpg_path.write_bytes(peak_jpeg)

    video_dur_s = sum(durations)
    audio_dur_s = len(pcm) / 2 / SAMPLE_RATE
    meta = {
        "id": stem_path.name,
        "file": mp4_path.name,
        "video_url": f"/recordings/events/{mp4_path.name}",
        "thumb_url": f"/recordings/events/{jpg_path.name}" if peak_jpeg else None,
        "started_at": started_at.isoformat(timespec="seconds"),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "frames": len(frames),
        "fps": round(len(frames) / video_dur_s, 2) if video_dur_s > 0 else 0,
        "duration_s": round(max(video_dur_s, audio_dur_s), 2),
        "video_duration_s": round(video_dur_s, 3),
        "audio_duration_s": round(audio_dur_s, 3),
        "av_offset_ms": round(offset_s * 1000, 2),
        "peak_motion_pct": peak_motion_pct,
        "peak_audio_pct": peak_audio_pct,
        "trigger_reason": trigger_reason,
        "chunks": len(chunks),
        "size_bytes": mp4_path.stat().st_size,
    }
    json_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    return meta


class Settings(BaseModel):
    monitoring_enabled: bool = True
    trigger_mode: TriggerMode = "either"
    motion_threshold_pct: float = Field(DEFAULT_MOTION_PCT, ge=0.5, le=30)
    audio_threshold_pct: float = Field(DEFAULT_AUDIO_PCT, ge=0.5, le=50)
    retention_days: int = Field(7, ge=1, le=30)
    max_recording_gb: float = Field(10, gt=0, allow_inf_nan=False)


def load_settings() -> Settings:
    try:
        return Settings.model_validate_json(CONFIG_PATH.read_bytes())
    except FileNotFoundError:
        return Settings()
    except (OSError, ValidationError):
        print("[CONFIG] 無法讀取 config.json，使用預設設定")
        return Settings()


def save_settings(settings: Settings):
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=CONFIG_PATH.parent, prefix=".config-", suffix=".tmp", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(settings.model_dump_json(indent=2).encode() + b"\n")
        os.replace(tmp_path, CONFIG_PATH)
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def cleanup_recordings(settings: Settings) -> int:
    cutoff = time.time() - settings.retention_days * 86400
    limit = settings.max_recording_gb * 1_000_000_000
    total_bytes = 0
    groups: dict[Path, list[tuple[Path, os.stat_result]]] = {}
    for path in RECORDINGS_ROOT.rglob("*"):
        try:
            if path.is_symlink() or not path.is_file():
                continue
            stat = path.stat()
        except FileNotFoundError:
            continue
        total_bytes += stat.st_size
        if path.suffix not in {".mp4", ".jpg", ".json"}:
            continue
        stem = path.with_suffix("")
        if path.parent == EVENTS_DIR and SAFE_EVENT_ID.fullmatch(stem.name.removesuffix(".tmp")):
            stem = stem.with_name(stem.name.removesuffix(".tmp"))
        elif path.parent != RECORDINGS_ROOT / "segments":
            continue
        groups.setdefault(stem, []).append((path, stat))

    removed = 0
    ordered = sorted(
        (min(stat.st_mtime for _, stat in files), stem)
        for stem, files in groups.items()
    )
    for oldest, stem in ordered:
        if oldest >= cutoff and total_bytes <= limit:
            break
        for path, stat in groups[stem]:
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                print(f"[CLEANUP] 無法刪除 {path}: {exc}")
                continue
            total_bytes -= stat.st_size
            removed += 1
    return removed


def load_existing_events() -> list[dict]:
    if not EVENTS_DIR.exists():
        return []
    items = []
    for jf in sorted(EVENTS_DIR.glob("evt_*.json")):
        try:
            meta = json.loads(jf.read_text())
            if (EVENTS_DIR / meta["file"]).exists():
                items.append(meta)
        except Exception:
            pass
    return items


class StreamState:
    def __init__(self):
        self.connected = False
        self.peer: str | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.settings = Settings()
        self.recordings_lock = asyncio.Lock()

        self.latest_jpeg: bytes | None = None
        self.frame_seq = 0
        self.frame_cond: asyncio.Condition | None = None
        self.ws_queues: set[asyncio.Queue] = set()

        self.expected_seq: int | None = None
        self.seq_gaps = 0
        self.crc_errors = 0
        self.esp_stat: dict = {}

        self.detector = MotionDetector()
        self.live_motion_pct = 0.0
        self.live_audio_pct = 0.0
        self.last_motion_at: str | None = None

        self.frames_buf: list[tuple[int, bytes, float]] = []
        self.audio_buf: list[tuple[int, bytes, float]] = []
        self.chunk_start_us: int | None = None
        self.chunk_wall_time: datetime | None = None

        self.pre_roll: MediaChunk | None = None
        self.active_event: ActiveEvent | None = None
        self.encoding_tasks: set[asyncio.Task] = set()
        self.events: list[dict] = []
        self.evt_counter = 0
        self.quiet_chunks_skipped = 0

    def _broadcast_ws(self, msg: bytes):
        for q in tuple(self.ws_queues):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass

    async def notify_new_frame(self):
        if self.frame_cond is not None:
            async with self.frame_cond:
                self.frame_cond.notify_all()

    def flush_and_reset(self):
        if (self.settings.trigger_mode == "continuous" and self.chunk_start_us is not None
                and self.frames_buf and self.audio_buf):
            last_audio_us, last_pcm, _ = self.audio_buf[-1]
            end_us = max(self.frames_buf[-1][0],
                         last_audio_us + len(last_pcm) // 2 * 1_000_000 // SAMPLE_RATE)
            self._process_media_chunk(
                self.chunk_start_us, end_us, self.chunk_wall_time or datetime.now(),
                self.frames_buf.copy(), self.audio_buf.copy(), None
            )
        if self.active_event and self.active_event.chunks:
            self._finalize_event(next_frame_us=None)
        self.frames_buf.clear()
        self.audio_buf.clear()
        self.chunk_start_us = None
        self.chunk_wall_time = None
        self.pre_roll = None
        self.detector.reset()
        self.live_motion_pct = 0.0
        self.live_audio_pct = 0.0

    def _matches_trigger_mode(self, chunk: MediaChunk) -> bool:
        if self.settings.trigger_mode == "continuous":
            return True
        if self.settings.trigger_mode == "video":
            return chunk.video_triggered
        if self.settings.trigger_mode == "audio":
            return chunk.audio_triggered
        if self.settings.trigger_mode == "both":
            return chunk.video_triggered and chunk.audio_triggered
        return chunk.video_triggered or chunk.audio_triggered

    def add_media(self, rtype: int, t_us: int, payload: bytes):
        if rtype == REC_FRAME:
            self.latest_jpeg = payload
            self.frame_seq += 1
            score = self.detector.score(payload)
            self.live_motion_pct = score
            if score >= self.settings.motion_threshold_pct:
                self.last_motion_at = datetime.now().isoformat(timespec="seconds")
            self.frames_buf.append((t_us, payload, score))
            self._broadcast_ws(b"\x02" + payload)
        elif rtype == REC_AUDIO:
            alevel = calc_audio_level_pct(payload)
            self.live_audio_pct = alevel
            self.audio_buf.append((t_us, payload, alevel))
            self._broadcast_ws(b"\x01" + payload)

        if self.chunk_start_us is None and self.frames_buf and self.audio_buf:
            self.chunk_start_us = min(self.frames_buf[0][0], self.audio_buf[0][0])
            self.chunk_wall_time = datetime.now()

        self._try_emit_chunks()

    def _try_emit_chunks(self):
        while self.chunk_start_us is not None and self.frames_buf and self.audio_buf:
            cut_us = self.chunk_start_us + CHUNK_US
            v_end = self.frames_buf[-1][0]
            last_a_t, last_a_pcm, _ = self.audio_buf[-1]
            a_end = last_a_t + len(last_a_pcm) // 2 * 1_000_000 // SAMPLE_RATE
            if v_end < cut_us or a_end < cut_us:
                break

            c_frames = [f for f in self.frames_buf if f[0] < cut_us]
            self.frames_buf = [f for f in self.frames_buf if f[0] >= cut_us]
            next_frame_us = self.frames_buf[0][0] if self.frames_buf else None

            c_audio: list[tuple[int, bytes, float]] = []
            rem_audio: list[tuple[int, bytes, float]] = []
            for a_t, pcm, alevel in self.audio_buf:
                nsamp = len(pcm) // 2
                c_end = a_t + nsamp * 1_000_000 // SAMPLE_RATE
                if c_end <= cut_us:
                    c_audio.append((a_t, pcm, alevel))
                elif a_t >= cut_us:
                    rem_audio.append((a_t, pcm, alevel))
                else:
                    split_samp = max(0, min(nsamp, round((cut_us - a_t) * SAMPLE_RATE / 1_000_000)))
                    split_byte = split_samp * 2
                    if split_byte > 0:
                        left = pcm[:split_byte]
                        c_audio.append((a_t, left, calc_audio_level_pct(left)))
                    if split_byte < len(pcm):
                        right = pcm[split_byte:]
                        rem_audio.append((cut_us, right, calc_audio_level_pct(right)))

            wall = self.chunk_wall_time or datetime.now()
            start_us = self.chunk_start_us
            self.audio_buf = rem_audio
            self.chunk_start_us = cut_us
            self.chunk_wall_time = datetime.now()

            if not c_frames or not c_audio:
                continue

            self._process_media_chunk(start_us, cut_us, wall, c_frames, c_audio, next_frame_us)

    def _process_media_chunk(
        self, start_us: int, end_us: int, wall: datetime,
        frames: list[tuple[int, bytes, float]], audio: list[tuple[int, bytes, float]],
        next_frame_us: int | None,
    ):
        v_thr = self.settings.motion_threshold_pct
        a_thr = self.settings.audio_threshold_pct
        motion_frames = sum(1 for _, _, score in frames if score >= v_thr)
        peak_frame = max(frames, key=lambda frame: frame[2])
        loud_chunks = sum(1 for _, _, level in audio if level >= a_thr)
        peak_audio = max(level for _, _, level in audio)
        self._process_chunk(MediaChunk(
            start_us=start_us,
            end_us=end_us,
            wall_time=wall,
            frames=frames,
            audio=audio,
            motion_frames=motion_frames,
            peak_motion_pct=peak_frame[2],
            peak_jpeg=peak_frame[1],
            audio_loud_chunks=loud_chunks,
            peak_audio_pct=peak_audio,
            video_triggered=motion_frames >= MOTION_MIN_FRAMES,
            audio_triggered=loud_chunks >= AUDIO_MIN_CHUNKS or peak_audio >= a_thr * 2.0,
        ), next_frame_us)

    def _process_chunk(self, chunk: MediaChunk, next_frame_us: int | None):
        if self._matches_trigger_mode(chunk):
            if self.active_event is None:
                initial = [self.pre_roll, chunk] if self.pre_roll is not None else [chunk]
                started = initial[0].wall_time
                self.active_event = ActiveEvent(
                    started_at=started,
                    chunks=initial,
                    post_roll_left=POST_ROLL_CHUNKS,
                    peak_motion_pct=chunk.peak_motion_pct,
                    peak_audio_pct=chunk.peak_audio_pct,
                    peak_jpeg=chunk.peak_jpeg,
                    by_video=chunk.video_triggered,
                    by_audio=chunk.audio_triggered,
                    continuous=self.settings.trigger_mode == "continuous",
                )
                self.pre_roll = None
                print(f"[TRIGGER:{self.settings.trigger_mode}] Event started "
                      f"(motion={chunk.peak_motion_pct}%, audio={chunk.peak_audio_pct}%)")
            else:
                self.active_event.chunks.append(chunk)
                self.active_event.post_roll_left = POST_ROLL_CHUNKS
                self.active_event.by_video = self.active_event.by_video or chunk.video_triggered
                self.active_event.by_audio = self.active_event.by_audio or chunk.audio_triggered
                if chunk.peak_motion_pct >= self.active_event.peak_motion_pct:
                    self.active_event.peak_motion_pct = chunk.peak_motion_pct
                    self.active_event.peak_jpeg = chunk.peak_jpeg
                if chunk.peak_audio_pct > self.active_event.peak_audio_pct:
                    self.active_event.peak_audio_pct = chunk.peak_audio_pct

            if len(self.active_event.chunks) >= MAX_EVENT_CHUNKS:
                self._finalize_event(next_frame_us)
        else:
            if self.active_event is not None:
                self.active_event.chunks.append(chunk)
                self.active_event.post_roll_left -= 1
                if self.active_event.post_roll_left <= 0:
                    self._finalize_event(next_frame_us)
                    self.pre_roll = chunk
            else:
                self.pre_roll = chunk
                self.quiet_chunks_skipped += 1

    def _finalize_event(self, next_frame_us: int | None):
        evt = self.active_event
        self.active_event = None
        if evt is None or not evt.chunks:
            return
        self.evt_counter += 1
        ts = evt.started_at.strftime("%Y%m%d_%H%M%S")
        stem = EVENTS_DIR / f"evt_{ts}_{self.evt_counter:04d}"
        if evt.continuous:
            reason = "持續錄影"
        elif evt.by_video and evt.by_audio:
            reason = "畫面+聲音"
        elif evt.by_audio:
            reason = "聲音"
        else:
            reason = "畫面"
        task = asyncio.create_task(self._encode_event(
            stem, evt.started_at, evt.chunks, next_frame_us,
            evt.peak_motion_pct, evt.peak_audio_pct, reason, evt.peak_jpeg
        ))
        self.encoding_tasks.add(task)
        task.add_done_callback(self.encoding_tasks.discard)

    async def _encode_event(self, stem: Path, started_at: datetime, chunks: list[MediaChunk],
                            next_frame_us: int | None, peak_motion_pct: float,
                            peak_audio_pct: float, reason: str, peak_jpeg: bytes):
        try:
            async with self.recordings_lock:
                meta = await asyncio.to_thread(
                    mux_event_sync, stem, started_at, chunks, next_frame_us,
                    peak_motion_pct, peak_audio_pct, reason, peak_jpeg
                )
                self.events.append(meta)
                print(f"[EVENT SAVED] {meta['file']} | {reason} | duration={meta['duration_s']}s "
                      f"| motion={peak_motion_pct}% audio={peak_audio_pct}% | {meta['size_bytes'] // 1024} KB")
                await self.cleanup()
        except Exception as e:
            print(f"[EVENT ERROR] {stem.name}: {e}")
    async def cleanup(self):
        try:
            removed = await asyncio.to_thread(cleanup_recordings, self.settings)
        except OSError as exc:
            print(f"[CLEANUP] 無法掃描錄影目錄: {exc}")
            removed = 0
        self.events = [event for event in self.events if (EVENTS_DIR / event["file"]).is_file()]
        if removed:
            print(f"[CLEANUP] 已清理 {removed} 個錄影檔案")

    def update_settings(self, **changes):
        settings = self.settings.model_copy(update=changes)
        try:
            save_settings(settings)
        except OSError as exc:
            raise HTTPException(status_code=500, detail="無法儲存監控設定") from exc
        if (self.settings.trigger_mode == "continuous") != (settings.trigger_mode == "continuous"):
            self.flush_and_reset()
        self.settings = settings


state = StreamState()


async def handle_esp32(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = str(writer.get_extra_info("peername"))
    print(f"[TCP] ESP32 connected from {peer}")
    if state.writer is not None:
        try:
            state.writer.close()
        except Exception:
            pass

    state.connected = True
    state.peer = peer
    state.writer = writer

    writer.write(b"1" if state.settings.monitoring_enabled else b"0")
    await writer.drain()

    try:
        while True:
            hdr_bytes = await reader.readexactly(PKT_HDR.size)
            magic, rtype, seq, t_us, length = PKT_HDR.unpack(hdr_bytes)
            if magic != MAGIC or length > 256 * 1024:
                raise ValueError(f"Invalid header magic={magic.hex()} len={length}")

            payload = await reader.readexactly(length)
            crc_bytes = await reader.readexactly(4)
            (crc,) = struct.unpack("<I", crc_bytes)
            if crc != zlib.crc32(payload):
                state.crc_errors += 1
                raise ValueError(f"CRC mismatch on seq={seq}")

            if state.expected_seq is not None and seq != state.expected_seq:
                if seq < state.expected_seq:
                    state.flush_and_reset()
                else:
                    state.seq_gaps += seq - state.expected_seq
            state.expected_seq = (seq + 1) & 0xFFFFFFFF

            if rtype in (REC_AUDIO, REC_FRAME):
                if not state.settings.monitoring_enabled:
                    continue
                state.add_media(rtype, t_us, payload)
                if rtype == REC_FRAME:
                    await state.notify_new_frame()
            elif rtype == REC_STAT:
                state.esp_stat = json.loads(payload.decode("utf-8", errors="replace"))
    except (asyncio.IncompleteReadError, ConnectionError):
        print(f"[TCP] ESP32 disconnected ({peer})")
    except Exception as e:
        print(f"[TCP ERROR] {peer}: {e}")
    finally:
        if state.writer is writer:
            state.connected = False
            state.writer = None
            if state.settings.trigger_mode == "continuous":
                state.flush_and_reset()
        writer.close()


async def periodic_cleanup_loop():
    while True:
        await asyncio.sleep(3600)
        async with state.recordings_lock:
            await state.cleanup()


@asynccontextmanager
async def lifespan(app: FastAPI):
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    state.settings = load_settings()
    state.recordings_lock = asyncio.Lock()
    state.frame_cond = asyncio.Condition()
    state.events = load_existing_events()
    if state.events:
        nums = [int(e["id"].rsplit("_", 1)[-1]) for e in state.events if "_" in e["id"]]
        state.evt_counter = max(nums, default=0)
    await state.cleanup()
    cleanup_task = asyncio.create_task(periodic_cleanup_loop())
    tcp_server = await asyncio.start_server(handle_esp32, TCP_HOST, TCP_PORT)
    print(f"[SERVER] ESP32 TCP {TCP_HOST}:{TCP_PORT} "
          f"| {len(state.events)} historical events")
    async with tcp_server:
        yield
    cleanup_task.cancel()
    state.flush_and_reset()
    await asyncio.gather(*state.encoding_tasks)


app = FastAPI(title="ESP32-S3 Surveillance Server", lifespan=lifespan)
EVENTS_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/recordings", StaticFiles(directory=str(RECORDINGS_ROOT)), name="recordings")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/status")
async def get_status():
    return {
        "connected": state.connected,
        "peer": state.peer,
        "monitoring_enabled": state.settings.monitoring_enabled,
        "recording_event": state.active_event is not None,
        "trigger_mode": state.settings.trigger_mode,
        "live_motion_pct": state.live_motion_pct,
        "motion_threshold_pct": state.settings.motion_threshold_pct,
        "live_audio_pct": state.live_audio_pct,
        "audio_threshold_pct": state.settings.audio_threshold_pct,
        "last_motion_at": state.last_motion_at,
        "seq_gaps": state.seq_gaps,
        "crc_errors": state.crc_errors,
        "quiet_chunks_skipped": state.quiet_chunks_skipped,
        "events_count": len(state.events),
        "latest_event": state.events[-1] if state.events else None,
        "retention_days": state.settings.retention_days,
        "max_recording_gb": state.settings.max_recording_gb,
        "esp32": state.esp_stat,
    }


@app.get("/api/events")
async def list_events():
    return {"events": list(reversed(state.events))}


@app.delete("/api/events/{event_id}")
async def delete_event(event_id: str):
    if not SAFE_EVENT_ID.match(event_id):
        raise HTTPException(status_code=400, detail="Invalid event_id")
    async with state.recordings_lock:
        removed = False
        for ext in (".mp4", ".jpg", ".json"):
            p = EVENTS_DIR / f"{event_id}{ext}"
            if p.exists():
                p.unlink()
                removed = True
        state.events = [e for e in state.events if e["id"] != event_id]
        if not removed:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"deleted": event_id}


@app.post("/api/monitoring")
async def set_monitoring(enabled: bool = Query(...)):
    state.update_settings(monitoring_enabled=enabled)
    if not enabled:
        state.flush_and_reset()
    if state.writer is not None:
        try:
            state.writer.write(b"1" if enabled else b"0")
            await state.writer.drain()
        except Exception:
            pass
    return JSONResponse({"monitoring_enabled": state.settings.monitoring_enabled})


@app.post("/api/trigger_config")
async def set_trigger_config(
    mode: str | None = Query(default=None),
    motion_pct: float | None = Query(default=None, ge=0.5, le=30.0),
    audio_pct: float | None = Query(default=None, ge=0.5, le=50.0),
    retention_days: int | None = Query(default=None, ge=1, le=30),
    max_recording_gb: float | None = Query(default=None, gt=0, allow_inf_nan=False),
):
    if mode is not None and mode not in get_args(TriggerMode):
        raise HTTPException(status_code=400, detail="Invalid trigger mode")
    changes = {
        "trigger_mode": mode,
        "motion_threshold_pct": round(motion_pct, 1) if motion_pct is not None else None,
        "audio_threshold_pct": round(audio_pct, 1) if audio_pct is not None else None,
        "retention_days": retention_days,
        "max_recording_gb": max_recording_gb,
    }
    changes = {key: value for key, value in changes.items() if value is not None}
    if retention_days is not None or max_recording_gb is not None:
        async with state.recordings_lock:
            state.update_settings(**changes)
            await state.cleanup()
    elif changes:
        state.update_settings(**changes)
    return state.settings.model_dump(exclude={"monitoring_enabled"})


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    if INGEST_PROTOCOL in ws.scope["subprotocols"]:
        await ws_ingest(ws)
        return
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=16)
    state.ws_queues.add(q)
    if state.latest_jpeg is not None and state.settings.monitoring_enabled:
        await ws.send_bytes(b"\x02" + state.latest_jpeg)
    try:
        while True:
            msg = await q.get()
            await ws.send_bytes(msg)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        state.ws_queues.discard(q)


@app.websocket("/ingest")
async def ws_ingest(ws: WebSocket):
    expected_token = os.environ.get("ESP32_INGEST_TOKEN", "").encode()
    supplied_token = ws.headers.get("x-esp32-token", "").encode()
    if (
        INGEST_PROTOCOL not in ws.scope["subprotocols"]
        or not expected_token
        or not hmac.compare_digest(expected_token, supplied_token)
    ):
        await ws.close(code=1008)
        return

    tcp_host = "127.0.0.1" if TCP_HOST == "0.0.0.0" else TCP_HOST
    try:
        reader, writer = await asyncio.open_connection(tcp_host, TCP_PORT)
    except OSError:
        await ws.close(code=1013)
        return

    async def send_controls():
        while control := await reader.read(1):
            await ws.send_bytes(control)

    async def receive_packets():
        while True:
            message = await asyncio.wait_for(ws.receive(), timeout=30)
            if message["type"] == "websocket.disconnect":
                print(f"[INGEST] WebSocket disconnected (code={message.get('code')})")
                return
            packet = message.get("bytes")
            if packet is None:
                await ws.close(code=1003)
                return
            if not PKT_HDR.size + 4 <= len(packet) <= MAX_INGEST_PACKET:
                print(f"[INGEST] Invalid message size: {len(packet)}")
                await ws.close(code=1009)
                return
            _, _, _, _, length = PKT_HDR.unpack_from(packet)
            if len(packet) != PKT_HDR.size + length + 4:
                print(f"[INGEST] Message length mismatch: actual={len(packet)}, payload={length}")
                await ws.close(code=1002)
                return
            writer.write(packet)
            await writer.drain()

    try:
        await ws.accept(subprotocol=INGEST_PROTOCOL)
        async with asyncio.TaskGroup() as group:
            upstream = group.create_task(receive_packets())
            downstream = group.create_task(send_controls())
            _, pending = await asyncio.wait(
                (upstream, downstream), return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
    except* (WebSocketDisconnect, OSError, TimeoutError):
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass


@app.get("/api/live")
async def live_mjpeg():
    """事件驅動 MJPEG 串流：每張結尾立即送出下一個 boundary，消除瀏覽器多憋一張造成的卡頓。"""
    async def gen():
        yield b"--frame\r\n"
        last_seq = -1
        while True:
            if state.frame_cond is None:
                await asyncio.sleep(0.05)
                continue
            async with state.frame_cond:
                if state.frame_seq == last_seq:
                    try:
                        await asyncio.wait_for(state.frame_cond.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        continue
                if state.frame_seq == last_seq or state.latest_jpeg is None:
                    continue
                last_seq = state.frame_seq
                jpg = state.latest_jpeg
            yield (
                b"Content-Type: image/jpeg\r\n"
                + f"Content-Length: {len(jpg)}\r\n\r\n".encode()
                + jpg
                + b"\r\n--frame\r\n"
            )

    return StreamingResponse(
        gen(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
