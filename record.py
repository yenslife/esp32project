import time
import wave
import serial

PORT = "/dev/cu.usbmodem2101"
SAMPLE_RATE = 16000
DURATION = 5
EXPECTED_BYTES = SAMPLE_RATE * DURATION * 2

with serial.Serial(PORT, 115200, timeout=2) as ser:
    print("Waiting for ESP32...")

    # 等待 ESP32 輸出 READY
    deadline = time.monotonic() + 15

    while time.monotonic() < deadline:
        line = ser.readline().decode(
            "ascii", errors="replace"
        ).strip()

        if line:
            print("ESP32:", line)

        if line == "READY":
            break
    else:
        raise RuntimeError("ESP32 did not send READY")

    print("Recording for 5 seconds...")
    ser.write(b"R")
    ser.flush()

    # 等待錄音完成的標頭
    ser.timeout = 10
    header = ser.readline().decode("ascii").strip()

    if not header.startswith("BEGIN "):
        raise RuntimeError(f"Unexpected header: {header}")

    size = int(header.split()[1])

    if size != EXPECTED_BYTES:
        raise RuntimeError(f"Unexpected PCM size: {size}")

    # 接收原始二進位 PCM
    ser.timeout = 5
    audio = bytearray()

    while len(audio) < size:
        chunk = ser.read(min(4096, size - len(audio)))

        if not chunk:
            raise TimeoutError(
                f"Only received {len(audio)}/{size} bytes"
            )

        audio.extend(chunk)

# 封裝成 WAV
with wave.open("recording.wav", "wb") as wav:
    wav.setnchannels(1)
    wav.setsampwidth(2)
    wav.setframerate(SAMPLE_RATE)
    wav.writeframes(audio)

print(f"Saved recording.wav ({len(audio)} PCM bytes)")
