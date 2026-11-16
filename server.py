# Mac 本機監控伺服器（階段 1：TCP 連續影音接收 + 10 秒同步 H.264/AAC 分段轉碼 + 即時狀態/畫面 API）
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import json
import os
import shutil
import struct
import subprocess
import tempfile
import time
import wave
import zlib
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse

TCP_PORT = 9000
SAMPLE_RATE = 16000
SEGMENT_US = 10_000_000  # 每 10 秒切一段 MP4

REC_AUDIO = 1
REC_FRAME = 2
REC_STAT = 3
MAGIC = b"\xA5\x5A"
PKT_HDR = struct.Struct("<2sBIqI")  # magic(2s), type(u8), seq(u32), t_us(i64), len(u32)

RECORDINGS_DIR = Path("recordings/segments")
FFMPEG = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"


def mux_segment_sync(out_path: Path, frames: list[tuple[int, bytes]],
                     next_frame_us: int | None, audio: list[tuple[int, bytes]]) -> dict:
    """在背景執行緒將 10 秒內的 VFR JPEG 與 PCM 合並轉碼為 H.264 + AAC MP4。"""
    RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="esp32seg_") as tmp:
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

        tmp_mp4 = out_path.with_suffix(".tmp.mp4")
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
            "-c:a", "aac", "-b:a", "64k",
            "-movflags", "+faststart",
            str(tmp_mp4),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        os.replace(tmp_mp4, out_path)

    video_dur_s = sum(durations)
    audio_dur_s = len(pcm) / 2 / SAMPLE_RATE
    return {
        "file": out_path.name,
        "frames": len(frames),
        "fps": round(len(frames) / video_dur_s, 2) if video_dur_s > 0 else 0,
        "video_duration_s": round(video_dur_s, 3),
        "audio_duration_s": round(audio_dur_s, 3),
        "av_offset_ms": round(offset_s * 1000, 2),
        "size_bytes": out_path.stat().st_size,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


class StreamState:
    def __init__(self):
        self.connected = False
        self.peer: str | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.monitoring_enabled = True
        self.latest_jpeg: bytes | None = None
        self.frame_event = asyncio.Event()

        self.expected_seq: int | None = None
        self.seq_gaps = 0
        self.crc_errors = 0
        self.esp_stat: dict = {}
        self.segments: list[dict] = []
        self.seg_counter = 0

        self.frames_buf: list[tuple[int, bytes]] = []
        self.audio_buf: list[tuple[int, bytes]] = []
        self.seg_start_us: int | None = None
        self.last_t_us: int | None = None

    def reset_buffers(self):
        self.frames_buf.clear()
        self.audio_buf.clear()
        self.seg_start_us = None

    def add_media(self, rtype: int, t_us: int, payload: bytes):
        if rtype == REC_FRAME:
            self.latest_jpeg = payload
            self.frame_event.set()
            self.frame_event.clear()
            self.frames_buf.append((t_us, payload))
        elif rtype == REC_AUDIO:
            self.audio_buf.append((t_us, payload))

        if self.seg_start_us is None and self.frames_buf and self.audio_buf:
            self.seg_start_us = min(self.frames_buf[0][0], self.audio_buf[0][0])

        self._try_emit_segments()

    def _try_emit_segments(self):
        while self.seg_start_us is not None and self.frames_buf and self.audio_buf:
            cut_us = self.seg_start_us + SEGMENT_US
            v_end = self.frames_buf[-1][0]
            last_a_t, last_a_pcm = self.audio_buf[-1]
            a_end = last_a_t + len(last_a_pcm) // 2 * 1_000_000 // SAMPLE_RATE
            if v_end < cut_us or a_end < cut_us:
                break

            # 切出 [seg_start_us, cut_us) 的影格
            seg_frames = [f for f in self.frames_buf if f[0] < cut_us]
            self.frames_buf = [f for f in self.frames_buf if f[0] >= cut_us]
            next_frame_us = self.frames_buf[0][0] if self.frames_buf else None

            # 切出 [seg_start_us, cut_us) 的音訊，跨越邊界的 chunk 精確依取樣點切開
            seg_audio: list[tuple[int, bytes]] = []
            rem_audio: list[tuple[int, bytes]] = []
            for a_t, pcm in self.audio_buf:
                nsamp = len(pcm) // 2
                c_end = a_t + nsamp * 1_000_000 // SAMPLE_RATE
                if c_end <= cut_us:
                    seg_audio.append((a_t, pcm))
                elif a_t >= cut_us:
                    rem_audio.append((a_t, pcm))
                else:
                    split_samp = max(0, min(nsamp, round((cut_us - a_t) * SAMPLE_RATE / 1_000_000)))
                    split_byte = split_samp * 2
                    if split_byte > 0:
                        seg_audio.append((a_t, pcm[:split_byte]))
                    if split_byte < len(pcm):
                        rem_audio.append((cut_us, pcm[split_byte:]))
            self.audio_buf = rem_audio
            self.seg_start_us = cut_us

            if seg_frames and seg_audio:
                self.seg_counter += 1
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                out_path = RECORDINGS_DIR / f"seg_{ts}_{self.seg_counter:04d}.mp4"
                asyncio.create_task(self._encode_segment(out_path, seg_frames, next_frame_us, seg_audio))

    async def _encode_segment(self, out_path: Path, frames, next_frame_us, audio):
        try:
            info = await asyncio.to_thread(mux_segment_sync, out_path, frames, next_frame_us, audio)
            self.segments.append(info)
            if len(self.segments) > 200:
                self.segments = self.segments[-200:]
            print(f"[SEGMENT] {info['file']} | {info['frames']} frames ({info['fps']} fps) "
                  f"| video={info['video_duration_s']}s audio={info['audio_duration_s']}s "
                  f"| offset={info['av_offset_ms']}ms | {info['size_bytes'] // 1024} KB")
        except Exception as e:
            print(f"[SEGMENT ERROR] {out_path.name}: {e}")


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

    # 連線建立立即同步目前監控開關狀態
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
                    # ESP32 重新開機（seq 歸零），清空舊時間軸的殘餘緩衝
                    state.reset_buffers()
                else:
                    state.seq_gaps += seq - state.expected_seq
            state.expected_seq = (seq + 1) & 0xFFFFFFFF

            if rtype in (REC_AUDIO, REC_FRAME):
                state.add_media(rtype, t_us, payload)
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
    tcp_server = await asyncio.start_server(handle_esp32, "0.0.0.0", TCP_PORT)
    print(f"[SERVER] Listening for ESP32 TCP stream on 0.0.0.0:{TCP_PORT}")
    async with tcp_server:
        yield


app = FastAPI(title="ESP32-S3 Surveillance Server", lifespan=lifespan)


@app.get("/api/status")
async def get_status():
    return {
        "connected": state.connected,
        "peer": state.peer,
        "monitoring_enabled": state.monitoring_enabled,
        "seq_gaps": state.seq_gaps,
        "crc_errors": state.crc_errors,
        "buffered_frames": len(state.frames_buf),
        "buffered_audio_chunks": len(state.audio_buf),
        "segments_count": len(state.segments),
        "latest_segment": state.segments[-1] if state.segments else None,
        "esp32": state.esp_stat,
    }


@app.get("/api/segments")
async def list_segments():
    return {"segments": list(reversed(state.segments[-50:]))}


@app.post("/api/monitoring")
async def set_monitoring(enabled: bool = Query(...)):
    state.monitoring_enabled = enabled
    if not enabled:
        state.reset_buffers()
    if state.writer is not None:
        try:
            state.writer.write(b"1" if enabled else b"0")
            await state.writer.drain()
        except Exception:
            pass
    return JSONResponse({"monitoring_enabled": state.monitoring_enabled})


@app.get("/api/live")
async def live_mjpeg():
    async def gen():
        last_sent = None
        while True:
            jpg = state.latest_jpeg
            if jpg is not None and jpg is not last_sent:
                last_sent = jpg
                yield (
                    b"--frame\r\nContent-Type: image/jpeg\r\n"
                    + f"Content-Length: {len(jpg)}\r\n\r\n".encode()
                    + jpg
                    + b"\r\n"
                )
            await asyncio.sleep(0.08)

    return StreamingResponse(gen(), media_type="multipart/x-mixed-replace; boundary=frame")
