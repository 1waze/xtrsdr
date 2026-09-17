set(RTLSDR_DIR "${CMAKE_SOURCE_DIR}/../../librtlsdr/src")

set(app_sources
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/convenience.c"

    "${RTLSDR_DIR}/librtlsdr.c"
    "${RTLSDR_DIR}/libusb.c"
    "${RTLSDR_DIR}/tuner_e4k.c"
    "${RTLSDR_DIR}/tuner_fc0012.c"
    "${RTLSDR_DIR}/tuner_fc0013.c"
    "${RTLSDR_DIR}/tuner_fc2580.c"
    "${RTLSDR_DIR}/tuner_r82xx.c"
)

idf_component_register(
    SRCS ${app_sources}

    INCLUDE_DIRS
        "."
        "${RTLSDR_DIR}"

    REQUIRES
        esp_eth
        esp_netif
        esp_event
        nvs_flash
        esp_driver_gpio
        esp_driver_uart
        ethernet_init
)

target_compile_options(${COMPONENT_LIB} PRIVATE
    -Wno-error=format
    -Wno-error=unused-const-variable
    -Wno-error=incompatible-pointer-types
    -Wno-error=switch
    -Wno-error
)
