# XIAO ESP32-S3 智慧監視器（本機 MVP）

使用 **Seeed Studio XIAO ESP32S3 Sense（OV3660 鏡頭 + 內建 PDM 麥克風）** 搭配 Mac 本機 FastAPI 伺服器的低成本智慧監視器系統。

## 核心功能

- **連續擷取與 Wi-Fi 串流**：ESP32 以 PSRAM Ring Buffer 解耦影音擷取與網路傳送。網路壅塞時，韌體會跳過等待超過約 0.75 秒的佇列封包，讓即時畫面盡快追上現場。被跳過的影音也會從這段錄影中略去。
- **輕量動態偵測**：伺服器將影格縮小為 `80×60` 灰階模糊圖計算相鄰影格差異（`< 0.1 ms/張`，無須安裝肥大的 OpenCV）。
- **前後緩衝事件錄影**：平時零硬碟寫入，僅在記憶體滾動保留最近 5 秒緩衝；偵測到動靜時，自動合併「**事件前 5 秒 + 動態期間 + 事件後 5 秒**」轉碼為單支 H.264 + AAC MP4（含高通濾波消除麥克風直流偏移）。
- **Web Dashboard（`http://localhost:8000`）**：
  - WebSocket 零緩衝即時畫面（Live View）與一鍵**即時聽音**
  - **監控模式 ON / OFF**（即時下發同步回 ESP32）
  - 即時變動比例條與靈敏度門檻滑桿
  - 歷史事件縮圖牆、內建播放器、單筆刪除，錄影時間以台北時間顯示
  - 歷史清單以藍色和琥珀色的 0–100 漸層色帶分別顯示畫面峰值與聲音峰值
  - 即時畫面與事件回放可順時針旋轉 0°、90°、180°、270°，角度儲存在目前的瀏覽器
  - 攝影機畫質可在「流暢」（320 × 240、8 FPS）與「清晰」（640 × 480、4 FPS）之間切換，選擇會儲存在伺服器
- **錄影自動清理**：預設保留 7 天，整個 `recordings/` 目錄以 10 GB 為上限。超過容量時，從最舊的錄影開始清理，事件影片、縮圖與紀錄會一起刪除。

## 快速啟動

### 1. 設定 Wi-Fi 與 Mac 區網 IP

複製範本並填入路由器 2.4GHz Wi-Fi 與 Mac 的區網 IP：

```sh
cp include/secrets.example.h include/secrets.h
```

### 2. 編譯並燒錄 ESP32 韌體

```sh
pio run -t upload
```

### 3. 啟動 Mac 本機伺服器與 Dashboard

```sh
uv run uvicorn server:app --host 0.0.0.0 --port 8000
```

瀏覽器開啟：**http://localhost:8000**

### 4. 調整錄影保留設定

在 Dashboard 調整監控開關、觸發模式、畫面與聲音門檻、保留天數。伺服器會將設定寫入本機的 `config.json`，下次啟動時自動載入。第一次儲存設定時會建立此檔案。

選擇「持續錄影」後，只要監控開啟並收到影音，伺服器就會錄影。影片約每 60 秒存成一段，在歷史清單標示「持續錄影」。關閉監控、切換到事件模式或攝影機斷線時，伺服器也會儲存目前收到的剩餘影音。這些影片與事件錄影共用保留天數與容量上限。

若要把錄影容量上限改為 5 GB，在伺服器運行時執行：

```sh
curl -f -X POST 'http://localhost:8000/api/trigger_config?max_recording_gb=5'
```

伺服器會在啟動、事件錄影完成、調整保留天數或容量上限，以及每小時檢查時清理。容量包含 `recordings/` 內的所有檔案，清理對象為事件檔案與舊版錄影片段。

## VM 反向代理

若 Nginx 與伺服器在同一台 VM，將 Dashboard 的上游設為 `127.0.0.1:8000`，並為 `/ws/live` 啟用 WebSocket 轉送。ESP32 的影音使用獨立的 TCP 連接埠 `9000`。可用 `ESP32_TCP_HOST` 設定 TCP 監聽位址，例如設成 `127.0.0.1`，再透過本機轉送服務接入。

### ESP32 透過 WSS 直接上傳

1. 為 VM 服務設定 `ESP32_INGEST_TOKEN`，使用每台攝影機部署時產生的隨機 token。Nginx 將 `/ingest` 以與 `/ws/live` 相同的 WebSocket 設定轉送給 VM。若反向代理已轉送 `/ws/live`，也可用這個路徑上傳，伺服器會用 WebSocket 子協定辨識攝影機。
2. 將 `include/ingest_secrets.example.h` 複製成 `include/ingest_secrets.h`，填入 HTTPS 網域、路徑、相同的 token，以及簽發站台憑證的根 CA PEM。此檔案已加入忽略規則。Wi‑Fi 帳密仍設定於 `include/secrets.h`。
3. 使用 USB 執行 `pio run -t upload`。序列監控顯示 `WSS ingest connected` 後，到 Dashboard 確認即時畫面。Wi‑Fi 需能連到站台的 443 埠，並讓 ESP32 透過 SNTP 校時。

韌體有 `include/ingest_secrets.h` 時使用 Espressif 的 WebSocket 用戶端，預設以 QVGA（320 × 240）串流。首次使用 Dashboard 畫質控制要先燒錄支援此功能的韌體；之後可以直接切換。解析度切換時，正在進行的錄影會另起一段。VM 的 `9000` 可只監聽本機。

## 目錄結構

```text
esp32project/
├── platformio.ini            # PlatformIO 硬體與 PSRAM 設定
├── include/
│   ├── secrets.example.h     # Wi-Fi 與伺服器位址範本
│   └── secrets.h             # 本機機敏設定（已加入 .gitignore）
├── src/
│   └── main.cpp              # ESP32 PSRAM Ring Buffer + Wi-Fi 串流韌體
├── server.py                 # FastAPI 接收、動態偵測、事件轉碼與清理伺服器
├── static/
│   └── index.html            # Web Dashboard 前端介面
├── capture.py / record.py    # 早期 USB 序列埠驗證腳本
└── recordings/events/        # 事件影片 (.mp4)、縮圖 (.jpg)、紀錄 (.json)
```
