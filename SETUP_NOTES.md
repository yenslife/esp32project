# Setup Notes

## Hardware

- Board: Seeed XIAO ESP32-S3 (PlatformIO board id: `seeed_xiao_esp32s3`)
- Chip: ESP32-S3 (QFN56) rev v0.2 — Wi-Fi, BT 5 (LE), dual core + LP core, 240MHz, embedded 8MB PSRAM
- USB mode: USB-Serial/JTAG
- MAC: 98:a3:16:f8:18:b4
- Serial port on this Mac: `/dev/cu.usbmodem2101`

## Tooling

- PlatformIO Core 6.2.0 installed as a uv tool (`uv tool install platformio`), providing `pio`, `piodebuggdb`, `platformio`
- PlatformIO project initialized with:

  ```sh
  pio project init --board seeed_xiao_esp32s3
  ```

  Installed platform: `espressif32@8.0.0`

- esptool（chip 燒錄/讀取用）由 PlatformIO 內建，不需要另外安裝
