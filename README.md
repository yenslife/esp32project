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
- **7 天自動清理**：啟動時與每小時背景清理超過 7 天的舊事件檔案。

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
