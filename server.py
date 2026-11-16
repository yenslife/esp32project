# Mac 本機監控伺服器（完整 MVP：即時無卡頓推播 + 畫面/聲音雙向動態偵測 + 前後 5 秒事件錄影 + 7 天清理 + Web Dashboard）
from array import array
import asyncio
from contextlib import asynccontextmanager
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

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image, ImageChops, ImageFilter

TCP_PORT = 9000
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

VALID_TRIGGER_MODES = {"video", "audio", "either", "both"}
RETENTION_DAYS = 7            # 自動保留最近 7 天

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


def cleanup_old_recordings(retention_days: float = RETENTION_DAYS) -> int:
    if not RECORDINGS_ROOT.exists():
        return 0
    cutoff = time.time() - retention_days * 86400
    removed = 0
    for sub in ("events", "segments"):
        d = RECORDINGS_ROOT / sub
        if not d.exists():
            continue
        for p in d.iterdir():
            if p.is_file() and p.suffix in {".mp4", ".jpg", ".json"}:
                try:
                    if p.stat().st_mtime < cutoff:
                        p.unlink()
                        removed += 1
                except OSError:
                    pass
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
        self.monitoring_enabled = True

        self.latest_jpeg: bytes | None = None
        self.frame_seq = 0
        self.frame_cond: asyncio.Condition | None = None
        self.ws_queues: set[asyncio.Queue] = set()

        self.expected_seq: int | None = None
        self.seq_gaps = 0
        self.crc_errors = 0
        self.esp_stat: dict = {}

        self.detector = MotionDetector()
        self.trigger_mode = "either"  # video | audio | either | both
        self.motion_threshold_pct = DEFAULT_MOTION_PCT
        self.audio_threshold_pct = DEFAULT_AUDIO_PCT
        self.retention_days = RETENTION_DAYS
        self.live_motion_pct = 0.0
        self.live_audio_pct = 0.0
        self.last_motion_at: str | None = None

        self.frames_buf: list[tuple[int, bytes, float]] = []
        self.audio_buf: list[tuple[int, bytes, float]] = []
        self.chunk_start_us: int | None = None
        self.chunk_wall_time: datetime | None = None

        self.pre_roll: MediaChunk | None = None
        self.active_event: ActiveEvent | None = None
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
        if self.trigger_mode == "video":
            return chunk.video_triggered
        if self.trigger_mode == "audio":
            return chunk.audio_triggered
        if self.trigger_mode == "both":
            return chunk.video_triggered and chunk.audio_triggered
        return chunk.video_triggered or chunk.audio_triggered

    def add_media(self, rtype: int, t_us: int, payload: bytes):
        if rtype == REC_FRAME:
            self.latest_jpeg = payload
            self.frame_seq += 1
            score = self.detector.score(payload)
            self.live_motion_pct = score
            if score >= self.motion_threshold_pct:
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

            v_thr = self.motion_threshold_pct
            a_thr = self.audio_threshold_pct
            motion_frames = sum(1 for _, _, s in c_frames if s >= v_thr)
            peak_frame = max(c_frames, key=lambda x: x[2])
            loud_chunks = sum(1 for _, _, a in c_audio if a >= a_thr)
            peak_audio = max(a for _, _, a in c_audio)

            v_trig = motion_frames >= MOTION_MIN_FRAMES
            a_trig = loud_chunks >= AUDIO_MIN_CHUNKS or peak_audio >= a_thr * 2.0

            chunk = MediaChunk(
                start_us=start_us,
                end_us=cut_us,
                wall_time=wall,
                frames=c_frames,
                audio=c_audio,
                motion_frames=motion_frames,
                peak_motion_pct=peak_frame[2],
                peak_jpeg=peak_frame[1],
                audio_loud_chunks=loud_chunks,
                peak_audio_pct=peak_audio,
                video_triggered=v_trig,
                audio_triggered=a_trig,
            )
            self._process_chunk(chunk, next_frame_us)

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
                )
                self.pre_roll = None
                print(f"[TRIGGER:{self.trigger_mode}] Event started "
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
        if evt.by_video and evt.by_audio:
            reason = "畫面+聲音"
        elif evt.by_audio:
            reason = "聲音"
        else:
            reason = "畫面"
        asyncio.create_task(self._encode_event(
            stem, evt.started_at, evt.chunks, next_frame_us,
            evt.peak_motion_pct, evt.peak_audio_pct, reason, evt.peak_jpeg
        ))

    async def _encode_event(self, stem: Path, started_at: datetime, chunks: list[MediaChunk],
                            next_frame_us: int | None, peak_motion_pct: float,
                            peak_audio_pct: float, reason: str, peak_jpeg: bytes):
        try:
            meta = await asyncio.to_thread(
                mux_event_sync, stem, started_at, chunks, next_frame_us,
                peak_motion_pct, peak_audio_pct, reason, peak_jpeg
            )
            self.events.append(meta)
            print(f"[EVENT SAVED] {meta['file']} | {reason} | duration={meta['duration_s']}s "
                  f"| motion={peak_motion_pct}% audio={peak_audio_pct}% | {meta['size_bytes'] // 1024} KB")
        except Exception as e:
            print(f"[EVENT ERROR] {stem.name}: {e}")


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

    writer.write(b"1" if state.monitoring_enabled else b"0")
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
        writer.close()


async def periodic_cleanup_loop():
    while True:
        removed = await asyncio.to_thread(cleanup_old_recordings, state.retention_days)
        if removed:
            valid_files = {p.name for p in EVENTS_DIR.glob("evt_*.mp4")}
            state.events = [e for e in state.events if e["file"] in valid_files]
            print(f"[CLEANUP] Removed {removed} expired files (> {state.retention_days} days)")
        await asyncio.sleep(3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    state.frame_cond = asyncio.Condition()
    state.events = load_existing_events()
    if state.events:
        nums = [int(e["id"].rsplit("_", 1)[-1]) for e in state.events if "_" in e["id"]]
        state.evt_counter = max(nums, default=0)
    cleanup_task = asyncio.create_task(periodic_cleanup_loop())
    tcp_server = await asyncio.start_server(handle_esp32, "0.0.0.0", TCP_PORT)
    print(f"[SERVER] Dashboard http://localhost:8000 | ESP32 TCP 0.0.0.0:{TCP_PORT} "
          f"| {len(state.events)} historical events")
    async with tcp_server:
        yield
    cleanup_task.cancel()
    state.flush_and_reset()


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
        "monitoring_enabled": state.monitoring_enabled,
        "recording_event": state.active_event is not None,
        "trigger_mode": state.trigger_mode,
        "live_motion_pct": state.live_motion_pct,
        "motion_threshold_pct": state.motion_threshold_pct,
        "live_audio_pct": state.live_audio_pct,
        "audio_threshold_pct": state.audio_threshold_pct,
        "last_motion_at": state.last_motion_at,
        "seq_gaps": state.seq_gaps,
        "crc_errors": state.crc_errors,
        "quiet_chunks_skipped": state.quiet_chunks_skipped,
        "events_count": len(state.events),
        "latest_event": state.events[-1] if state.events else None,
        "retention_days": state.retention_days,
        "esp32": state.esp_stat,
    }


@app.get("/api/events")
async def list_events():
    return {"events": list(reversed(state.events))}


@app.delete("/api/events/{event_id}")
async def delete_event(event_id: str):
    if not SAFE_EVENT_ID.match(event_id):
        raise HTTPException(status_code=400, detail="Invalid event_id")
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
    state.monitoring_enabled = enabled
    if not enabled:
        state.flush_and_reset()
    if state.writer is not None:
        try:
            state.writer.write(b"1" if enabled else b"0")
            await state.writer.drain()
        except Exception:
            pass
    return JSONResponse({"monitoring_enabled": state.monitoring_enabled})


@app.post("/api/trigger_config")
async def set_trigger_config(
    mode: str | None = Query(default=None),
    motion_pct: float | None = Query(default=None, ge=0.5, le=30.0),
    audio_pct: float | None = Query(default=None, ge=0.5, le=50.0),
    retention_days: int | None = Query(default=None, ge=1, le=30),
):
    if mode is not None:
        if mode not in VALID_TRIGGER_MODES:
            raise HTTPException(status_code=400, detail="Invalid trigger mode")
        state.trigger_mode = mode
    if motion_pct is not None:
        state.motion_threshold_pct = round(motion_pct, 1)
    if audio_pct is not None:
        state.audio_threshold_pct = round(audio_pct, 1)
    if retention_days is not None:
        state.retention_days = retention_days
        removed = await asyncio.to_thread(cleanup_old_recordings, state.retention_days)
        if removed:
            valid_files = {p.name for p in EVENTS_DIR.glob("evt_*.mp4")}
            state.events = [e for e in state.events if e["file"] in valid_files]
    return {
        "trigger_mode": state.trigger_mode,
        "motion_threshold_pct": state.motion_threshold_pct,
        "audio_threshold_pct": state.audio_threshold_pct,
        "retention_days": state.retention_days,
    }


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=16)
    state.ws_queues.add(q)
    if state.latest_jpeg is not None and state.monitoring_enabled:
        await ws.send_bytes(b"\x02" + state.latest_jpeg)
    try:
        while True:
            msg = await q.get()
            await ws.send_bytes(msg)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        state.ws_queues.discard(q)


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
