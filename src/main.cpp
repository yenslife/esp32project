// XIAO ESP32S3 Sense 智慧監視器韌體（階段 1：PSRAM Ring Buffer + Wi-Fi 連續影音串流）
#include <Arduino.h>
#include <atomic>
#include <ESP_I2S.h>
#include <WiFi.h>
#include "esp_camera.h"
#include "esp_heap_caps.h"
#include "esp_log.h"
#include "esp_rom_crc.h"
#include "esp_timer.h"
#include "secrets.h"
#if __has_include("ingest_secrets.h")
#include "ingest_secrets.h"
#include "esp_websocket_client.h"
#include <time.h>
#endif

// XIAO ESP32S3 Sense 鏡頭腳位
#define PWDN_GPIO_NUM  -1
#define RESET_GPIO_NUM -1
#define XCLK_GPIO_NUM  10
#define SIOD_GPIO_NUM  40
#define SIOC_GPIO_NUM  39
#define Y9_GPIO_NUM    48
#define Y8_GPIO_NUM    11
#define Y7_GPIO_NUM    12
#define Y6_GPIO_NUM    14
#define Y5_GPIO_NUM    16
#define Y4_GPIO_NUM    18
#define Y3_GPIO_NUM    17
#define Y2_GPIO_NUM    15
#define VSYNC_GPIO_NUM 38
#define HREF_GPIO_NUM  47
#define PCLK_GPIO_NUM  13

constexpr uint32_t SAMPLE_RATE  = 16000;
constexpr size_t   AUDIO_CHUNK  = 4096;               // ~128ms PCM
constexpr int      JPEG_QUALITY = 22;                 // 數字越大，JPEG 壓縮越高
constexpr size_t   RING_SIZE    = 4 * 1024 * 1024;    // 4 MB PSRAM Ring Buffer
constexpr int64_t  MAX_QUEUE_AGE_US = 750000;         // 丟棄等待太久的封包，讓畫面追上現場
constexpr size_t   MAX_REC_SIZE = 128 * 1024;         // 單筆封包暫存上限
constexpr int SMOOTH_PROFILE = 'Q';
constexpr int DETAIL_PROFILE = 'V';

std::atomic<int> pendingCameraProfile{0};
std::atomic<int> appliedCameraProfile{SMOOTH_PROFILE};
std::atomic<bool> cameraProfileError{false};

enum RecType : uint8_t { REC_AUDIO = 1, REC_FRAME = 2, REC_STAT = 3 };

struct __attribute__((packed)) PacketHdr {
    uint8_t  magic[2];   // 0xA5, 0x5A
    uint8_t  type;
    uint32_t seq;
    int64_t  t_us;
    uint32_t len;
};

I2SClass I2S;
SemaphoreHandle_t ringMux = nullptr;

uint8_t* ringBuf    = nullptr;
size_t   ringHead   = 0;
size_t   ringTail   = 0;
size_t   ringUsed   = 0;
uint32_t nextSeq    = 0;

uint8_t* txStageBuf = nullptr;
size_t   txStageLen = 0;
size_t   txStagePos = 0;

volatile bool     monitoringEnabled = true;
volatile uint32_t ringDropped  = 0;
volatile uint32_t audioErrors  = 0;
volatile uint32_t grabErrors   = 0;
volatile uint32_t framesSent   = 0;
volatile uint32_t chunksSent   = 0;

static void ringWriteRaw(const uint8_t* src, size_t n) {
    size_t first = min(n, RING_SIZE - ringHead);
    memcpy(ringBuf + ringHead, src, first);
    if (n > first) {
        memcpy(ringBuf, src + first, n - first);
    }
    ringHead = (ringHead + n) % RING_SIZE;
    ringUsed += n;
}

static void ringReadRaw(uint8_t* dst, size_t n) {
    size_t first = min(n, RING_SIZE - ringTail);
    memcpy(dst, ringBuf + ringTail, first);
    if (n > first) {
        memcpy(dst + first, ringBuf, n - first);
    }
    ringTail = (ringTail + n) % RING_SIZE;
    ringUsed -= n;
}

bool pushRecord(uint8_t type, int64_t tUs, const uint8_t* payload, uint32_t len) {
    const size_t total = sizeof(PacketHdr) + len + sizeof(uint32_t);
    if (total > MAX_REC_SIZE) return false;

    // 先在鎖外算好 CRC32，避免佔用互斥鎖時間
    const uint32_t crc = esp_rom_crc32_le(0, payload, len);

    if (xSemaphoreTake(ringMux, pdMS_TO_TICKS(20)) != pdTRUE) {
        ringDropped = ringDropped + 1;
        return false;
    }
    if (RING_SIZE - ringUsed < total) {
        ringDropped = ringDropped + 1;
        xSemaphoreGive(ringMux);
        return false;
    }

    PacketHdr hdr = { { 0xA5, 0x5A }, type, nextSeq++, tUs, len };
    ringWriteRaw((const uint8_t*)&hdr, sizeof(hdr));
    ringWriteRaw(payload, len);
    ringWriteRaw((const uint8_t*)&crc, sizeof(crc));
    xSemaphoreGive(ringMux);
    return true;
}

// 從 Ring Buffer 一次取出一整筆完整封包到 txStageBuf，確保斷線重連永遠對齊封包邊界
bool popWholeRecord() {
    if (xSemaphoreTake(ringMux, pdMS_TO_TICKS(10)) != pdTRUE) return false;

    const int64_t oldestAllowedUs = esp_timer_get_time() - MAX_QUEUE_AGE_US;
    while (ringUsed >= sizeof(PacketHdr)) {
        PacketHdr hdr;
        ringReadRaw((uint8_t*)&hdr, sizeof(hdr));
        const size_t bodySize = hdr.len + sizeof(uint32_t);
        const size_t total = sizeof(hdr) + bodySize;
        if (hdr.magic[0] != 0xA5 || hdr.magic[1] != 0x5A ||
            total > MAX_REC_SIZE || bodySize > ringUsed) {
            // 寫入端會在鎖內放入完整封包。資料異常時清空緩衝區。
            ringHead = ringTail = ringUsed = 0;
            break;
        }
        const int64_t capturedUntilUs = hdr.type == REC_AUDIO
            ? hdr.t_us + (int64_t)(hdr.len / 2) * 1000000LL / SAMPLE_RATE
            : hdr.t_us;
        if (hdr.type != REC_STAT && capturedUntilUs < oldestAllowedUs) {
            ringTail = (ringTail + bodySize) % RING_SIZE;
            ringUsed -= bodySize;
            ringDropped = ringDropped + 1;
            continue;
        }

        memcpy(txStageBuf, &hdr, sizeof(hdr));
        ringReadRaw(txStageBuf + sizeof(hdr), bodySize);
        xSemaphoreGive(ringMux);
        txStageLen = total;
        txStagePos = 0;
        return true;
    }

    xSemaphoreGive(ringMux);
    return false;
}

// Core 0：高優先權連續讀取 PDM 麥克風，不因網路或攝影機阻塞
void audioTask(void*) {
    static uint8_t pcm[AUDIO_CHUNK];
    while (true) {
        size_t n = I2S.readBytes((char*)pcm, sizeof(pcm));
        int64_t tEnd = esp_timer_get_time();
        if (n == 0) {
            audioErrors = audioErrors + 1;
            vTaskDelay(pdMS_TO_TICKS(5));
            continue;
        }
        if (!monitoringEnabled) continue;

        int64_t tStart = tEnd - (int64_t)(n / 2) * 1000000LL / SAMPLE_RATE;
        if (pushRecord(REC_AUDIO, tStart, pcm, (uint32_t)n)) {
            chunksSent = chunksSent + 1;
        }
    }
}

// Core 1：持續取出 camera fb 避免 FB-OVF，依目標幀率推入 Ring Buffer
void cameraTask(void*) {
    int64_t nextFrameUs = esp_timer_get_time();
    int64_t frameIntervalUs = 125000;
    unsigned settlingFrames = 0;
    sensor_t* sensor = esp_camera_sensor_get();
    while (true) {
        const int requested = pendingCameraProfile.exchange(0);
        if (requested && requested != appliedCameraProfile.load()) {
            const bool detail = requested == DETAIL_PROFILE;
            if (sensor->set_framesize(sensor, detail ? FRAMESIZE_VGA : FRAMESIZE_QVGA) == 0) {
                frameIntervalUs = detail ? 250000 : 125000;
                nextFrameUs = esp_timer_get_time();
                settlingFrames = 2;
                appliedCameraProfile.store(requested);
                cameraProfileError.store(false);
            } else {
                cameraProfileError.store(true);
                Serial.println("ERROR: camera profile change failed");
            }
        } else if (requested) {
            cameraProfileError.store(false);
        }

        camera_fb_t* fb = esp_camera_fb_get();
        int64_t t = esp_timer_get_time();
        if (!fb) {
            grabErrors = grabErrors + 1;
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }

        if (settlingFrames > 0) {
            --settlingFrames;
        } else if (monitoringEnabled && t >= nextFrameUs) {
            nextFrameUs += frameIntervalUs;
            if (t > nextFrameUs) nextFrameUs = t;
            if (pushRecord(REC_FRAME, t, fb->buf, (uint32_t)fb->len)) {
                framesSent = framesSent + 1;
            }
        }
        esp_camera_fb_return(fb);
        vTaskDelay(pdMS_TO_TICKS(2));
    }
}

bool initCamera() {
    camera_config_t c = {};
    c.ledc_channel = LEDC_CHANNEL_0;
    c.ledc_timer   = LEDC_TIMER_0;
    c.pin_d0 = Y2_GPIO_NUM; c.pin_d1 = Y3_GPIO_NUM;
    c.pin_d2 = Y4_GPIO_NUM; c.pin_d3 = Y5_GPIO_NUM;
    c.pin_d4 = Y6_GPIO_NUM; c.pin_d5 = Y7_GPIO_NUM;
    c.pin_d6 = Y8_GPIO_NUM; c.pin_d7 = Y9_GPIO_NUM;
    c.pin_xclk = XCLK_GPIO_NUM; c.pin_pclk = PCLK_GPIO_NUM;
    c.pin_vsync = VSYNC_GPIO_NUM; c.pin_href = HREF_GPIO_NUM;
    c.pin_sccb_sda = SIOD_GPIO_NUM; c.pin_sccb_scl = SIOC_GPIO_NUM;
    c.pin_pwdn = PWDN_GPIO_NUM; c.pin_reset = RESET_GPIO_NUM;
    c.xclk_freq_hz = 20000000;
    c.pixel_format = PIXFORMAT_JPEG;
    // 先以最大尺寸配置緩衝區，之後由感測器切換解析度。
    c.frame_size   = FRAMESIZE_VGA;
    c.jpeg_quality = JPEG_QUALITY;
    c.fb_count     = 2;
    c.fb_location  = CAMERA_FB_IN_PSRAM;
    c.grab_mode    = CAMERA_GRAB_LATEST;
    if (esp_camera_init(&c) != ESP_OK) return false;
    sensor_t* sensor = esp_camera_sensor_get();
    if (sensor && sensor->set_framesize(sensor, FRAMESIZE_QVGA) == 0) return true;
    esp_camera_deinit();
    return false;
}

bool initMic() {
    I2S.setPinsPdmRx(42, 41);
    return I2S.begin(I2S_MODE_PDM_RX, SAMPLE_RATE,
                     I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_MONO);
}

void ensureWiFi() {
    if (WiFi.status() == WL_CONNECTED) return;
    Serial.printf("Connecting Wi-Fi (%s)...\n", WIFI_SSID);
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);  // 關閉省電睡眠以維持穩定吞吐量與低延遲
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    uint32_t start = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - start < 15000) {
        delay(300);
    }
    if (WiFi.status() == WL_CONNECTED) {
        Serial.printf("Wi-Fi connected: %s (RSSI %d dBm)\n",
                      WiFi.localIP().toString().c_str(), WiFi.RSSI());
    }
}

int64_t lastStatUs = 0;
int lastReportedProfile = 0;
bool lastReportedProfileError = false;
#ifdef ESP32_INGEST_HOST
esp_websocket_client_handle_t cameraSocket = nullptr;
std::atomic<int> pendingMonitorCommand{0};
bool timeSyncStarted = false;
#else
WiFiClient client;
uint32_t lastConnectTryMs = 0;
bool useFallbackHost = false;
#endif

void setMonitoring(int command) {
    if (command == '1' && !monitoringEnabled) {
        if (xSemaphoreTake(ringMux, pdMS_TO_TICKS(20)) == pdTRUE) {
            ringHead = ringTail = ringUsed = 0;
            xSemaphoreGive(ringMux);
        }
        txStageLen = txStagePos = 0;
        monitoringEnabled = true;
        Serial.println("Monitoring: ON");
    } else if (command == '0' && monitoringEnabled) {
        monitoringEnabled = false;
        Serial.println("Monitoring: OFF");
    }
}

#ifdef ESP32_INGEST_HOST
bool connectIngest() {
    if (!timeSyncStarted) {
        configTime(0, 0, "time.cloudflare.com", "time.google.com", "pool.ntp.org");
        timeSyncStarted = true;
    }
    // TLS 驗證需要正確的時間，等待 SNTP 校時。
    if (time(nullptr) < 1735689600) return false;
    if (!cameraSocket) {
        esp_websocket_client_config_t config = {};
        config.uri = "wss://" ESP32_INGEST_HOST ESP32_INGEST_PATH;
        config.cert_pem = ESP32_INGEST_CA;
        config.subprotocol = "esp32-ingest-v1";
        config.headers = "X-ESP32-Token: " ESP32_INGEST_TOKEN "\r\n";
        config.reconnect_timeout_ms = 2000;
        config.network_timeout_ms = 2500;
        config.enable_close_reconnect = true;
        cameraSocket = esp_websocket_client_init(&config);
        if (!cameraSocket) return false;

        esp_err_t error = esp_websocket_register_events(
            cameraSocket, WEBSOCKET_EVENT_ANY,
            [](void*, esp_event_base_t, int32_t event, void* data) {
                const auto* message = static_cast<const esp_websocket_event_data_t*>(data);
                if (event == WEBSOCKET_EVENT_CONNECTED) {
                    Serial.println("WSS ingest connected");
                } else if (event == WEBSOCKET_EVENT_DISCONNECTED) {
                    Serial.println("WSS ingest disconnected");
                } else if (event == WEBSOCKET_EVENT_ERROR) {
                    Serial.printf("WSS error type=%d TLS=%d socket=%d HTTP=%d\n",
                                  message->error_handle.error_type,
                                  message->error_handle.esp_tls_stack_err,
                                  message->error_handle.esp_transport_sock_errno,
                                  message->error_handle.esp_ws_handshake_status_code);
                } else if (event == WEBSOCKET_EVENT_DATA && message->op_code == 2 &&
                           message->payload_len == 1 && message->data_len == 1) {
                    const int command = message->data_ptr[0];
                    if (command == SMOOTH_PROFILE || command == DETAIL_PROFILE) {
                        pendingCameraProfile.store(command);
                    } else {
                        pendingMonitorCommand.store(command);
                    }
                }
            }, nullptr
        );
        if (error != ESP_OK || esp_websocket_client_start(cameraSocket) != ESP_OK) {
            esp_websocket_client_destroy(cameraSocket);
            cameraSocket = nullptr;
            return false;
        }
    }
    setMonitoring(pendingMonitorCommand.exchange(0));
    return esp_websocket_client_is_connected(cameraSocket);
}
#endif

void setup() {
    Serial.begin(115200);
    delay(1000);
    esp_log_level_set("cam_hal", ESP_LOG_NONE);

    ringMux = xSemaphoreCreateMutex();
    ringBuf = (uint8_t*)heap_caps_malloc(RING_SIZE, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    txStageBuf = (uint8_t*)heap_caps_malloc(MAX_REC_SIZE, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!ringMux || !ringBuf || !txStageBuf) {
        Serial.println("ERROR: PSRAM allocation failed");
        return;
    }

    if (!initCamera()) {
        Serial.println("ERROR: camera init failed");
        return;
    }
    if (!initMic()) {
        Serial.println("ERROR: mic init failed");
        return;
    }

    ensureWiFi();

    xTaskCreatePinnedToCore(audioTask,  "audio",  8192, nullptr, 10, nullptr, 0);
    xTaskCreatePinnedToCore(cameraTask, "camera", 8192, nullptr,  8, nullptr, 1);
    Serial.println("STREAMER_READY");
}

void loop() {
    if (!ringBuf) {
        delay(1000);
        return;
    }

    ensureWiFi();
    if (WiFi.status() != WL_CONNECTED) {
        delay(500);
        return;
    }

#ifdef ESP32_INGEST_HOST
    if (!connectIngest()) {
        delay(20);
        return;
    }
#else
    if (!client.connected()) {
        // 若上一筆送一半斷線，重設起點讓重連後重送完整封包
        txStagePos = 0;
        if (millis() - lastConnectTryMs < 1500) {
            delay(50);
            return;
        }
        lastConnectTryMs = millis();
        const char* host = useFallbackHost ? SERVER_HOST_FALLBACK : SERVER_HOST;
        Serial.printf("Connecting server %s:%d...\n", host, SERVER_TCP_PORT);
        if (!client.connect(host, SERVER_TCP_PORT, 3000)) {
            useFallbackHost = !useFallbackHost;
            return;
        }
        client.setNoDelay(true);
        Serial.printf("Server connected (%s:%d)\n", host, SERVER_TCP_PORT);
    }

    // 接收伺服器下發的監控開關（'1'=開啟, '0'=暫停）
    while (client.available()) {
        const int command = client.read();
        if (command == SMOOTH_PROFILE || command == DETAIL_PROFILE) {
            pendingCameraProfile.store(command);
        } else {
            setMonitoring(command);
        }
    }
#endif

    // 每 5 秒送出狀態，畫質套用結果有變時立即回報。
    int64_t nowUs = esp_timer_get_time();
    const int cameraProfile = appliedCameraProfile.load();
    const bool profileError = cameraProfileError.load();
    if (nowUs - lastStatUs >= 5000000LL ||
        cameraProfile != lastReportedProfile || profileError != lastReportedProfileError) {
        char statJson[288];
        int n = snprintf(
            statJson, sizeof(statJson),
            "{\"monitoring\":%s,\"ring_used\":%u,\"ring_size\":%u,"
            "\"ring_dropped\":%lu,\"frames_sent\":%lu,\"chunks_sent\":%lu,"
            "\"audio_errors\":%lu,\"grab_errors\":%lu,\"rssi\":%d,"
            "\"camera_profile\":\"%s\",\"camera_profile_error\":%s}",
            monitoringEnabled ? "true" : "false",
            (unsigned)ringUsed, (unsigned)RING_SIZE,
            (unsigned long)ringDropped, (unsigned long)framesSent,
            (unsigned long)chunksSent, (unsigned long)audioErrors,
            (unsigned long)grabErrors, WiFi.RSSI(),
            cameraProfile == DETAIL_PROFILE ? "detail" : "smooth",
            profileError ? "true" : "false"
        );
        if (n > 0 && (size_t)n < sizeof(statJson) &&
            pushRecord(REC_STAT, nowUs, (const uint8_t*)statJson, (uint32_t)n)) {
            lastStatUs = nowUs;
            lastReportedProfile = cameraProfile;
            lastReportedProfileError = profileError;
            Serial.printf("STAT %s\n", statJson);
        }
    }

    // 取出一筆完整封包。TCP 模式分段寫入，WSS 模式每個訊息送一筆。
    if (txStagePos >= txStageLen) {
        if (!popWholeRecord()) {
            delay(2);
            return;
        }
    }

#ifdef ESP32_INGEST_HOST
    const int sent = esp_websocket_client_send_bin(
        cameraSocket, reinterpret_cast<const char*>(txStageBuf),
        txStageLen, pdMS_TO_TICKS(500)
    );
    if (sent != static_cast<int>(txStageLen)) {
        Serial.printf("WSS send failed (%u bytes)\n", (unsigned)txStageLen);
    }
    txStagePos = txStageLen;
#else
    size_t remain = txStageLen - txStagePos;
    size_t chunk = min(remain, (size_t)16384);
    int written = client.write(txStageBuf + txStagePos, chunk);
    if (written > 0) {
        txStagePos += (size_t)written;
    } else {
        client.stop();
        txStagePos = 0;
    }
#endif
}
