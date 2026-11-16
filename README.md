# esp32project — XIAO ESP32S3 Sense 影音同時錄製測試

OV3660 攝影機 + PDM 麥克風同時錄 10 秒，透過 USB Serial 傳回 Mac，重建為可播放的音訊/影片。

## 使用

```sh
pio run -t upload        # 編譯 + 燒錄（上傳前先關 serial monitor）
uv run capture.py        # 觸發錄製並接收資料
open output/combined.mp4 # 播放
```

`output/` 產出：`audio.wav`、`video.mp4`（VFR）、`combined.mp4`、`metrics.json`、`frames/*.jpg`。

## 架構

- **韌體**（`src/main.cpp`）：共用時鐘 `esp_timer_get_time()`。音訊由 FreeRTOS task（core 0, priority 10）連續讀 I2S 4096B chunk 進 PSRAM buffer，每 chunk 記錄估算開始時間；攝影機在 main loop（core 1）以 10 FPS 節拍取 JPEG frame 存進 PSRAM arena。錄滿 10 秒後用二進位 record（type + t_us + len + payload + CRC32）序列化送出，最後送 metrics JSON + `DONE`。
- **接收端**（`capture.py`）：驗 CRC → audio 接成 WAV → frames 寫檔後用 ffmpeg concat demuxer 依時間戳重建 VFR mp4 → `-itsoffset` 對齊 audio → `combined.mp4`。

## 記憶體

PSRAM 8MB：audio buffer ~352KB + frame arena（剩餘 PSRAM - 256KB margin，實測用 ~1.1MB / 100 VGA JPEG）。arena 滿會計 `frames_dropped` 而不是沉默丟棄。

## metrics.json 欄位

`capture_us` `frames` `frames_dropped` `grab_errors` `audio_bytes` `audio_chunks` `audio_errors` `audio_overruns` `arena_used` `arena_total` `actual_fps` `frame_interval_us{min,max,mean}` `audio_duration_s` `av_start_offset_ms` `audio_gap_count` `audio_max_gap_ms`

## 已知限制

- Audio chunk 時間戳 = 讀完時間 − 理論時長，非 DMA 硬體時間；`av_start_offset_ms`（實測 ~132ms）含此估算誤差，不宜視為真實同步精度。
- Frame 時間戳是 fb 取回時間，比實際曝光晚（~數 ms～VSYNC 週期）。
- 傳輸在錄完後才開始，非即時串流。

## 下一步

- 提高 frame 時間戳精度（用 VSYNC/ESP32 cam 內部時間戳）
- JPEG → H.264 即時編碼減少記憶體
- microSD 落地支援更長錄製
