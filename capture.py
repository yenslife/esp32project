# 接收 ESP32 的二進位 records，輸出 output/{audio.wav, video.mp4, combined.mp4, metrics.json}
import json
import shutil
import struct
import subprocess
import sys
import time
import wave
import zlib
from pathlib import Path

import serial

PORT = "/dev/cu.usbmodem2101"
SAMPLE_RATE = 16000
OUT = Path("output")
FRAMES_DIR = OUT / "frames"

REC_AUDIO = 1
REC_FRAME = 2
REC_END = 0xFF
STREAM_MAGIC = b"\xA5\x5A\xF0\x0F"
HDR = struct.Struct("<BqI")  # type u8, t_us i64（可為負）, len u32


def read_exact(ser, n, timeout=30):
    ser.timeout = timeout
    buf = bytearray()
    while len(buf) < n:
        chunk = ser.read(n - len(buf))
        if not chunk:
            raise TimeoutError(f"Serial timeout: got {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


def main():
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    if not (1 <= duration <= 60):
        raise ValueError("duration 必須介於 1～60 秒")

    audio = []   # (t_us, pcm bytes)
    frames = []  # (t_us, jpeg bytes)

    with serial.Serial(PORT, 115200, timeout=2) as ser:
        # 等串流靜止，排掉上一輪可能殘留的文字或資料
        quiet_since = time.monotonic()
        deadline = quiet_since + 30
        ser.timeout = 0.3
        while time.monotonic() - quiet_since < 0.8:
            if ser.read(4096):
                quiet_since = time.monotonic()
            if time.monotonic() > deadline:
                raise RuntimeError("Serial never went quiet")

        print(f"Requesting {duration}s capture...")
        ser.write(f"R{duration}\n".encode())
        ser.flush()

        # 等待 BEGIN_STREAM 標記（期間可印出 ACK 或錯誤訊息，忽略 cam_hal 雜訊）
        ser.timeout = 2.0
        stream_deadline = time.monotonic() + duration + 25
        while True:
            if time.monotonic() > stream_deadline:
                raise TimeoutError("Timed out waiting for BEGIN_STREAM")
            raw = ser.readline()
            if not raw:
                continue
            line = raw.decode("ascii", errors="replace").strip()
            if not line:
                continue
            if line == "BEGIN_STREAM":
                break
            if line.startswith("ERROR:"):
                raise RuntimeError(f"ESP32 {line}")
            if line.startswith("ACK ") or line == "READY":
                print(f"ESP32: {line}")

        magic = read_exact(ser, len(STREAM_MAGIC), timeout=10)
        if magic != STREAM_MAGIC:
            raise RuntimeError(f"Stream magic mismatch: {magic.hex()}")

        nbytes = 0
        while True:
            hdr = read_exact(ser, HDR.size, timeout=30)
            rtype, t_us, length = HDR.unpack(hdr)
            nbytes += HDR.size
            if rtype == REC_END:
                break
            if rtype not in (REC_AUDIO, REC_FRAME) or length > 4 << 20:
                raise RuntimeError(
                    f"Bad record at stream offset {nbytes}: "
                    f"type={rtype:#x} t={t_us} len={length} hdr={hdr.hex()}"
                )
            payload = read_exact(ser, length)
            crc = struct.unpack("<I", read_exact(ser, 4))[0]
            if crc != zlib.crc32(payload):
                raise RuntimeError(f"CRC mismatch on record type {rtype}")
            (audio if rtype == REC_AUDIO else frames).append((t_us, payload))
            nbytes += length + 4

        ser.timeout = 10
        metrics_line = ser.readline().decode("ascii").strip()
        metrics = json.loads(metrics_line)
        tail = ser.readline().decode("ascii").strip()
        if tail != "DONE":
            raise RuntimeError(f"Expected DONE, got: {tail}")

    total_pcm_bytes = sum(len(p) for _, p in audio)
    print(f"Received {len(frames)} frames, {len(audio)} audio chunks "
          f"({total_pcm_bytes} bytes)")
    if metrics.get("frames_dropped", 0) > 0 or metrics.get("audio_overruns", 0) > 0:
        print(f"WARNING: frames_dropped={metrics.get('frames_dropped')}, "
              f"audio_overruns={metrics.get('audio_overruns')}")

    OUT.mkdir(exist_ok=True)
    if FRAMES_DIR.exists():
        shutil.rmtree(FRAMES_DIR)
    FRAMES_DIR.mkdir()

    # audio.wav：依時間戳順序串接
    audio.sort(key=lambda x: x[0])
    pcm = b"".join(p for _, p in audio)
    with wave.open(str(OUT / "audio.wav"), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(pcm)

    if not frames:
        raise RuntimeError("No video frames received")

    # video.mp4：用時間戳差計算每張影格持續時間（VFR）
    frames.sort(key=lambda x: x[0])
    for i, (_, jpg) in enumerate(frames):
        (FRAMES_DIR / f"f{i:04d}.jpg").write_bytes(jpg)

    durations = []
    for i in range(len(frames)):
        if i + 1 < len(frames):
            d = max((frames[i + 1][0] - frames[i][0]) / 1e6, 0.001)
        else:
            d = durations[-1] if durations else 0.1
        durations.append(d)

    concat = FRAMES_DIR / "concat.txt"
    with concat.open("w") as f:
        for i, d in enumerate(durations):
            f.write(f"file 'f{i:04d}.jpg'\n")
            f.write(f"duration {d:.6f}\n")
        f.write(f"file 'f{len(frames) - 1:04d}.jpg'\n")

    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    subprocess.run(
        [ffmpeg, "-y", "-f", "concat", "-safe", "0", "-i", str(concat),
         "-fps_mode", "vfr", "-pix_fmt", "yuv420p", str(OUT / "video.mp4")],
        check=True, capture_output=True,
    )

    # combined.mp4：offset_s = video_start - audio_start
    # offset_s > 0 代表影片比音訊晚開始 → 延遲 video.mp4
    # offset_s < 0 代表音訊比影片晚開始 → 延遲 audio.wav
    audio_start_us = audio[0][0] if audio else 0
    video_start_us = frames[0][0]
    offset_s = (video_start_us - audio_start_us) / 1e6
    cmd = [ffmpeg, "-y"]
    if offset_s >= 0:
        cmd += ["-itsoffset", f"{offset_s:.6f}", "-i", str(OUT / "video.mp4"),
                "-i", str(OUT / "audio.wav")]
    else:
        cmd += ["-i", str(OUT / "video.mp4"),
                "-itsoffset", f"{-offset_s:.6f}", "-i", str(OUT / "audio.wav")]
    subprocess.run(
        cmd + ["-c:v", "copy", "-c:a", "aac", str(OUT / "combined.mp4")],
        check=True, capture_output=True,
    )

    frame_times = [t for t, _ in frames]
    intervals = [frame_times[i + 1] - frame_times[i]
                 for i in range(len(frame_times) - 1)]
    video_span_s = ((frame_times[-1] - frame_times[0]) / 1e6 + durations[-1]
                    if frames else 0.0)

    audio_times = [t for t, _ in audio]
    # gap > 0 為音訊缺口，gap < 0 為時間戳估算抖動重疊
    audio_gaps = [
        audio_times[i + 1] - audio_times[i]
        - len(audio[i][1]) / 2 * 1e6 / SAMPLE_RATE
        for i in range(len(audio_times) - 1)
    ]
    pos_gaps = [g for g in audio_gaps if g > 0]
    neg_gaps = [-g for g in audio_gaps if g < 0]

    metrics.update({
        "received_audio_bytes": total_pcm_bytes,
        "received_frames": len(frames),
        "video_duration_s": round(video_span_s, 3),
        "audio_duration_s": round(len(pcm) / 2 / SAMPLE_RATE, 3),
        "actual_fps": round((len(frames) - 1) / ((frame_times[-1] - frame_times[0]) / 1e6), 2)
                      if len(frame_times) > 1 else 0,
        "frame_interval_us": {
            "min": min(intervals),
            "max": max(intervals),
            "mean": round(sum(intervals) / len(intervals), 1),
        } if intervals else {},
        "audio_start_ms": round(audio_start_us / 1000, 2),
        "video_start_ms": round(video_start_us / 1000, 2),
        "av_start_offset_ms": round((video_start_us - audio_start_us) / 1000, 2),
        "audio_gap_count": sum(1 for g in pos_gaps if g > 20000),
        "audio_max_gap_ms": round(max(pos_gaps, default=0) / 1000, 2),
        "audio_max_overlap_ms": round(max(neg_gaps, default=0) / 1000, 2),
    })
    (OUT / "metrics.json").write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False))
    print("Wrote output/: audio.wav video.mp4 combined.mp4 metrics.json")


if __name__ == "__main__":
    main()
