#pragma once

#define ESP32_INGEST_HOST "lab-cam.yenslife.top"
#define ESP32_INGEST_PATH "/ingest"
#define ESP32_INGEST_TOKEN "REPLACE_WITH_DEVICE_TOKEN"
inline constexpr char ESP32_INGEST_CA[] = R"PEM(-----BEGIN CERTIFICATE-----
REPLACE_WITH_ROOT_CA_PEM
-----END CERTIFICATE-----
)PEM";
