# XIAO ESP32-S3 智慧監視器（本機 MVP）

使用 **Seeed Studio XIAO ESP32S3 Sense（OV3660 鏡頭 + 內建 PDM 麥克風）** 搭配 Mac 本機 FastAPI 伺服器的低成本智慧監視器系統。

## 核心功能

- **連續擷取與 Wi-Fi 串流**：ESP32 以 4 MB PSRAM Ring Buffer 解耦影音擷取與網路傳送，斷線自動重連不掉封包。
- **輕量動態偵測**：伺服器將影格縮小為 `80×60` 灰階模糊圖計算相鄰影格差異（`< 0.1 ms/張`，無須安裝肥大的 OpenCV）。
- **前後緩衝事件錄影**：平時零硬碟寫入，僅在記憶體滾動保留最近 5 秒緩衝；偵測到動靜時，自動合併「**事件前 5 秒 + 動態期間 + 事件後 5 秒**」轉碼為單支 H.264 + AAC MP4（含高通濾波消除麥克風直流偏移）。
- **Web Dashboard（`http://localhost:8000`）**：
  - WebSocket 零緩衝即時畫面（Live View）與一鍵**即時聽音**
  - **監控模式 ON / OFF**（即時下發同步回 ESP32）
  - 即時變動比例條與靈敏度門檻滑桿
  - 歷史事件縮圖牆、內建播放器、單筆刪除
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

若要把錄影容量上限改為 5 GB，在伺服器運行時執行：

```sh
curl -f -X POST 'http://localhost:8000/api/trigger_config?max_recording_gb=5'
```

伺服器會在啟動、事件錄影完成、調整保留天數或容量上限，以及每小時檢查時清理。容量包含 `recordings/` 內的所有檔案，清理對象為事件檔案與舊版錄影片段。

## VM 反向代理

若 Nginx 與伺服器在同一台 VM，將 Dashboard 的上游設為 `127.0.0.1:8000`，並為 `/ws/live` 啟用 WebSocket 轉送。ESP32 的影音使用獨立的 TCP 連接埠 `9000`。可用 `ESP32_TCP_HOST` 設定 TCP 監聽位址，例如設成 `127.0.0.1`，再透過本機轉送服務接入。

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
