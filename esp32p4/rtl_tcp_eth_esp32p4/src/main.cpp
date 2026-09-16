/*
 * rtl-sdr, turns your Realtek RTL2832 based DVB dongle into a SDR receiver
 * Copyright (C) 2012 by Steve Markgraf <steve@steve-m.de>
 * Copyright (C) 2012-2013 by Hoernchen <la@tfc-server.de>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
//#include "driver/spi_master.h"
#include "esp_eth.h"
#define LOG_LOCAL_LEVEL ESP_LOG_VERBOSE
#include "esp_log.h"
#include "esp_system.h"
#include "esp_heap_caps.h"
#include "esp_netif.h"
#include "esp_eth.h"
#include "esp_event.h"
//#include "nvs_flash.h"
#include "ethernet_init.h"

#include "driver/gpio.h"
#include "driver/uart.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "lwip/err.h"
#include "lwip/sockets.h"
#include "lwip/sys.h"
#include <lwip/netdb.h>


#include <fcntl.h>
#include <pthread.h>

#include <libusb.h>
#include "rtl-sdr.h"
#include "convenience.h"

#define TAG "main"

#define UART_NUM UART_NUM_0       
#define UART_TX_PIN GPIO_NUM_17
#define UART_RX_PIN GPIO_NUM_18


//const gpio_num_t LED_GPIO = GPIO_NUM_15;
//int led_state = 1;

/*
#define W5500_SPI_HOST SPI2_HOST
#define W5500_SCLK_GPIO 18
#define W5500_MISO_GPIO 19
#define W5500_MOSI_GPIO 23
#define W5500_CS_GPIO 5
#define W5500_INT_GPIO 4
#define W5500_RST_GPIO -1 // Установите -1 если не используете reset
*/

#define closesocket close
#define SOCKADDR struct sockaddr
#define SOCKET int
#define SOCKET_ERROR -1

#define DEFAULT_PORT_STR "1234"
#define DEFAULT_SAMPLE_RATE_HZ 240000

/* Mobile Ethernet mode: ESP32-P4 is 192.168.50.1/24 and runs DHCP server. */
#define MOBILE_ETH_IP_A 192
#define MOBILE_ETH_IP_B 168
#define MOBILE_ETH_IP_C 50
#define MOBILE_ETH_IP_D 1

/* Fixed IQ ring buffer.  Four 5120-byte USB callbacks are coalesced into one
 * 20480-byte TCP chunk.  No malloc/free is performed while streaming. */
#define IQ_USB_BLOCK_SIZE 5120
#define IQ_BLOCKS_PER_CHUNK 4
#define IQ_CHUNK_SIZE (IQ_USB_BLOCK_SIZE * IQ_BLOCKS_PER_CHUNK)
#ifdef CONFIG_SPIRAM
#define IQ_RING_SLOTS 24
#else
#define IQ_RING_SLOTS 8
#endif

static SOCKET s;

static pthread_t tcp_worker_thread;
static pthread_t command_thread;
static pthread_cond_t exit_cond;
static pthread_mutex_t exit_cond_lock;

static pthread_mutex_t ll_mutex;
static pthread_cond_t cond;

static uint8_t *iq_ring = NULL;
static size_t iq_ring_len[IQ_RING_SLOTS];
static unsigned int iq_ring_head = 0;
static unsigned int iq_ring_tail = 0;
static unsigned int iq_ring_count = 0;
static size_t iq_build_len = 0;

typedef struct { /* structure size must be multiple of 2 bytes */
	char magic[4];
	uint32_t tuner_type;
	uint32_t tuner_gain_count;
} dongle_info_t;

static rtlsdr_dev_t *dev = NULL;

static int enable_biastee = 0;
static int global_numq = 0;
static unsigned long iq_dropped_blocks = 0;
static unsigned long iq_completed_chunks = 0;

static volatile int do_exit = 0;



/** Event handler for Ethernet events */
static void eth_event_handler(void *arg, esp_event_base_t event_base,
                              int32_t event_id, void *event_data)
{
    esp_netif_t *mobile_netif = (esp_netif_t *)arg;
    uint8_t mac_addr[6] = {0};
    /* we can get the ethernet driver handle from event data */
    esp_eth_handle_t eth_handle = *(esp_eth_handle_t *)event_data;

    switch (event_id) {
    case ETHERNET_EVENT_CONNECTED: {
        esp_eth_ioctl(eth_handle, ETH_CMD_G_MAC_ADDR, mac_addr);
        ESP_LOGI(TAG, "Ethernet Link Up");
        ESP_LOGI(TAG, "Ethernet HW Addr %02x:%02x:%02x:%02x:%02x:%02x",
                 mac_addr[0], mac_addr[1], mac_addr[2], mac_addr[3], mac_addr[4], mac_addr[5]);

        eth_speed_t speed;
        eth_duplex_t duplex;

        if (esp_eth_ioctl(eth_handle, ETH_CMD_G_SPEED, &speed) == ESP_OK) {
            ESP_LOGI(TAG, "Ethernet speed: %s",
                     speed == ETH_SPEED_100M ? "100 Mbps" :
                     speed == ETH_SPEED_10M  ? "10 Mbps"  : "unknown");
        } else {
            ESP_LOGW(TAG, "Could not read Ethernet speed");
        }

        if (esp_eth_ioctl(eth_handle, ETH_CMD_G_DUPLEX_MODE, &duplex) == ESP_OK) {
            ESP_LOGI(TAG, "Ethernet duplex: %s",
                     duplex == ETH_DUPLEX_FULL ? "full" : "half");
        } else {
            ESP_LOGW(TAG, "Could not read Ethernet duplex mode");
        }

        /* Ethernet interfaces normally use a DHCP client. In mobile mode this
         * netif was created with ESP_NETIF_DHCP_SERVER instead. Unlike the
         * Wi-Fi AP default netif, the Ethernet event path does not implicitly
         * start a DHCP server, so start it explicitly once the link is up. */
        if (mobile_netif != NULL) {
            esp_netif_dhcp_status_t dhcps_status = ESP_NETIF_DHCP_INIT;
            esp_err_t st = esp_netif_dhcps_get_status(mobile_netif, &dhcps_status);
            ESP_LOGD(TAG, "DHCP server status before start: err=%s status=%d",
                     esp_err_to_name(st), (int)dhcps_status);

            if (st == ESP_OK && dhcps_status != ESP_NETIF_DHCP_STARTED) {
                esp_err_t err = esp_netif_dhcps_start(mobile_netif);
                if (err == ESP_OK || err == ESP_ERR_ESP_NETIF_DHCP_ALREADY_STARTED) {
                    ESP_LOGI(TAG, "DHCP server started on 192.168.50.1");
                } else {
                    ESP_LOGE(TAG, "Failed to start DHCP server: %s (0x%x)",
                             esp_err_to_name(err), (unsigned)err);
                }
            }

            dhcps_status = ESP_NETIF_DHCP_INIT;
            st = esp_netif_dhcps_get_status(mobile_netif, &dhcps_status);
            ESP_LOGD(TAG, "DHCP server status after start: err=%s status=%d",
                     esp_err_to_name(st), (int)dhcps_status);

            esp_netif_ip_info_t info = {};
            if (esp_netif_get_ip_info(mobile_netif, &info) == ESP_OK) {
                ESP_LOGI(TAG, "Mobile ETH IP:" IPSTR " mask:" IPSTR " gw:" IPSTR,
                         IP2STR(&info.ip), IP2STR(&info.netmask), IP2STR(&info.gw));
            }
        }
        break;
    }
    case ETHERNET_EVENT_DISCONNECTED:
        ESP_LOGI(TAG, "Ethernet Link Down");
        break;
    case ETHERNET_EVENT_START:
        ESP_LOGI(TAG, "Ethernet Started");
        break;
    case ETHERNET_EVENT_STOP:
        ESP_LOGI(TAG, "Ethernet Stopped");
        break;
    default:
        break;
    }
}

/** Event handler for IP_EVENT_ETH_GOT_IP */
static void got_ip_event_handler(void *arg, esp_event_base_t event_base,
                                 int32_t event_id, void *event_data)
{
    ip_event_got_ip_t *event = (ip_event_got_ip_t *) event_data;
    const esp_netif_ip_info_t *ip_info = &event->ip_info;

    ESP_LOGI(TAG, "Ethernet Got IP Address");
    ESP_LOGI(TAG, "~~~~~~~~~~~");
    ESP_LOGI(TAG, "ETHIP:" IPSTR, IP2STR(&ip_info->ip));
    ESP_LOGI(TAG, "ETHMASK:" IPSTR, IP2STR(&ip_info->netmask));
    ESP_LOGI(TAG, "ETHGW:" IPSTR, IP2STR(&ip_info->gw));
    ESP_LOGI(TAG, "~~~~~~~~~~~");
}



void sighandler(void){
	rtlsdr_cancel_async(dev);
	fprintf(stderr, "ght, exiting!\n");
	do_exit = 1;			
}


void rtlsdr_callback(unsigned char *buf, uint32_t len, void *ctx)
{
    if (do_exit)
        return;

    if (len > IQ_CHUNK_SIZE) {
        iq_dropped_blocks++;
        return;
    }

    pthread_mutex_lock(&ll_mutex);

    /* The producer owns iq_ring_head until a complete chunk is published.
     * If all published slots are occupied, drop the incoming USB block rather
     * than overwriting a slot which the TCP worker may currently be sending. */
    if (iq_ring_count >= IQ_RING_SLOTS) {
        iq_dropped_blocks++;
        pthread_mutex_unlock(&ll_mutex);
        return;
    }

    /* Normally len is 5120.  If a callback would cross the chunk boundary,
     * publish the partial chunk first and continue in the next slot. */
    if (iq_build_len + len > IQ_CHUNK_SIZE) {
        iq_ring_len[iq_ring_head] = iq_build_len;
        iq_ring_head = (iq_ring_head + 1) % IQ_RING_SLOTS;
        iq_ring_count++;
        iq_completed_chunks++;
        iq_build_len = 0;
        pthread_cond_signal(&cond);

        if (iq_ring_count >= IQ_RING_SLOTS) {
            iq_dropped_blocks++;
            pthread_mutex_unlock(&ll_mutex);
            return;
        }
    }

    memcpy(iq_ring + ((size_t)iq_ring_head * IQ_CHUNK_SIZE) + iq_build_len, buf, len);
    iq_build_len += len;

    if (iq_build_len == IQ_CHUNK_SIZE) {
        iq_ring_len[iq_ring_head] = iq_build_len;
        iq_ring_head = (iq_ring_head + 1) % IQ_RING_SLOTS;
        iq_ring_count++;
        iq_completed_chunks++;
        iq_build_len = 0;

        if ((int)iq_ring_count > global_numq) {
            global_numq = (int)iq_ring_count;
            ESP_LOGD(TAG, "IQ ring high-water: %d/%d", global_numq, IQ_RING_SLOTS);
        }
        pthread_cond_signal(&cond);
    }

    pthread_mutex_unlock(&ll_mutex);
}




static void *tcp_worker(void *arg)
{
    struct timespec ts;
    struct timeval tp;
    int r = 0;

    while (1) {
        if (do_exit)
            pthread_exit(0);

        pthread_mutex_lock(&ll_mutex);
        while (iq_ring_count == 0 && !do_exit) {
            gettimeofday(&tp, NULL);
            ts.tv_sec = tp.tv_sec + 5;
            ts.tv_nsec = tp.tv_usec * 1000;
            r = pthread_cond_timedwait(&cond, &ll_mutex, &ts);
            if (r == ETIMEDOUT && iq_ring_count == 0) {
                pthread_mutex_unlock(&ll_mutex);
                ESP_LOGW(TAG, "IQ worker timeout");
                sighandler();
                pthread_exit(NULL);
            }
        }
        if (do_exit) {
            pthread_mutex_unlock(&ll_mutex);
            pthread_exit(0);
        }

        /* Keep this slot reserved while send() is using it. The producer
         * cannot overwrite it even if TCP blocks briefly. */
        unsigned int slot = iq_ring_tail;
        size_t chunk_len = iq_ring_len[slot];
        pthread_mutex_unlock(&ll_mutex);

        size_t offset = 0;
        while (offset < chunk_len) {
            ssize_t sent = send(s,
                                iq_ring + ((size_t)slot * IQ_CHUNK_SIZE) + offset,
                                chunk_len - offset,
                                0);
            if (sent <= 0 || do_exit) {
                ESP_LOGI(TAG, "TCP client disconnected");
                sighandler();
                pthread_exit(NULL);
            }
            offset += (size_t)sent;
        }

        pthread_mutex_lock(&ll_mutex);
        iq_ring_len[slot] = 0;
        iq_ring_tail = (iq_ring_tail + 1) % IQ_RING_SLOTS;
        if (iq_ring_count > 0)
            iq_ring_count--;
        pthread_mutex_unlock(&ll_mutex);
    }
}


static int set_gain_by_index(rtlsdr_dev_t *_dev, unsigned int index)
{
	int res = 0;
	int* gains;
	int count = rtlsdr_get_tuner_gains(_dev, NULL);

	if (count > 0 && (unsigned int)count > index) {
		gains = (int*) malloc(count* sizeof(int));
		count = rtlsdr_get_tuner_gains(_dev, gains);

		res = rtlsdr_set_tuner_gain(_dev, gains[index]);

		free(gains);
	}

	return res;
}

struct command{
	unsigned char cmd;
	unsigned int param;
}__attribute__((packed));



static void *command_worker(void *arg)
{
	int left, received = 0;
	fd_set readfds;
	struct command cmd={0, 0};
	struct timeval tv= {1, 0};
	int r = 0;
	uint32_t tmp;

	while(1) {
		left=sizeof(cmd);
		while(left >0) {
			FD_ZERO(&readfds);
			FD_SET(s, &readfds);
			tv.tv_sec = 1;
			tv.tv_usec = 0;
			r = select(s+1, &readfds, NULL, NULL, &tv);
			if(r) {
				received = recv(s, (char*)&cmd+(sizeof(cmd)-left), left, 0);
				left -= received;
			}
			if(received == SOCKET_ERROR || do_exit) {
				printf("comm recv bye\n");
				sighandler();
				pthread_exit(NULL);
			}
		}
		switch(cmd.cmd) {
		case 0x01:
			ESP_LOGD(TAG, "set freq %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_center_freq(dev,ntohl(cmd.param));
			break;
		case 0x02:
			ESP_LOGD(TAG, "set sample rate %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_sample_rate(dev, ntohl(cmd.param));
			break;
		case 0x03:
			ESP_LOGD(TAG, "set gain mode %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_tuner_gain_mode(dev, ntohl(cmd.param));
			break;
		case 0x04:
			ESP_LOGD(TAG, "set gain %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_tuner_gain(dev, ntohl(cmd.param));
			break;
		case 0x05:
			ESP_LOGD(TAG, "set freq correction %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_freq_correction(dev, ntohl(cmd.param));
			break;
		case 0x06:
			tmp = ntohl(cmd.param);
			ESP_LOGD(TAG, "set if stage %d gain %d", tmp >> 16, (short)(tmp & 0xffff));
			rtlsdr_set_tuner_if_gain(dev, tmp >> 16, (short)(tmp & 0xffff));
			break;
		case 0x07:
			ESP_LOGD(TAG, "set test mode %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_testmode(dev, ntohl(cmd.param));
			break;
		case 0x08:
			ESP_LOGD(TAG, "set agc mode %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_agc_mode(dev, ntohl(cmd.param));
			break;
		case 0x09:
			ESP_LOGD(TAG, "set direct sampling %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_direct_sampling(dev, ntohl(cmd.param));
			break;
		case 0x0a:
			ESP_LOGD(TAG, "set offset tuning %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_offset_tuning(dev, ntohl(cmd.param));
			break;
		case 0x0b:
			ESP_LOGD(TAG, "set rtl xtal %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_xtal_freq(dev, ntohl(cmd.param), 0);
			break;
		case 0x0c:
			ESP_LOGD(TAG, "set tuner xtal %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_xtal_freq(dev, 0, ntohl(cmd.param));
			break;
		case 0x0d:
			ESP_LOGD(TAG, "set tuner gain by index %lu", (unsigned long)ntohl(cmd.param));
			set_gain_by_index(dev, ntohl(cmd.param));
			break;
		case 0x0e:
			ESP_LOGD(TAG, "set bias tee %lu", (unsigned long)ntohl(cmd.param));
			rtlsdr_set_bias_tee(dev, (int)ntohl(cmd.param));
			break;
		default:
			break;
		}
		cmd.cmd = 0xff;
	}
}


static void print_memory_info() {
    ESP_LOGI(TAG, "Memory Information:");
    ESP_LOGI(TAG, "Total heap: %d bytes", esp_get_free_heap_size());
    ESP_LOGI(TAG, "Minimum free heap: %d bytes", esp_get_minimum_free_heap_size());
    ESP_LOGI(TAG, "Free PSRAM: %d bytes", heap_caps_get_free_size(MALLOC_CAP_SPIRAM));
	ESP_LOGI(TAG, "Free SRAM: %d bytes", heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
}


extern "C"  void app_main() {
	int r, opt, i;
	const char *addr = "127.0.0.1";
	const char *port = DEFAULT_PORT_STR;
	int netPort = 1234;
	uint32_t frequency = 100000000, samp_rate = DEFAULT_SAMPLE_RATE_HZ;
	struct sockaddr_storage local, remote;
	struct addrinfo *ai;
	struct addrinfo *aiHead;
	struct addrinfo  hints = {};
	char hostinfo[256];  //NI_MAXHOST
	char portinfo[256]; //NI_MAXSERV
	char remhostinfo[256];  //NI_MAXHOST
	char remportinfo[256];  //NI_MAXSERV
	int aiErr;
	uint32_t buf_num = 20;
	int dev_index = 0;
	int dev_given = 0;
	int gain = 0;
	int ppm_error = 0;
	int direct_sampling = 0;
	pthread_attr_t attr;
	void *status;
	struct timeval tv = {1,0};
	struct linger ling = {1,0};
	SOCKET listensocket = 0;
	socklen_t rlen;
	fd_set readfds;
	u_long blockmode = 1;
	dongle_info_t dongle_info;

//	ESP_ERROR_CHECK(uart_driver_install(UART_NUM_0, 256, 0, 0, NULL, 0));
//	ESP_ERROR_CHECK(uart_set_pin(UART_NUM_0, UART_TX_PIN, UART_RX_PIN, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE));

	printf("xtrsdr/rtl_tcp_eth_esp32p4 v1.1.1-mobile start\n");  
	esp_log_level_set("*", ESP_LOG_INFO);

	print_memory_info();

#ifdef CONFIG_SPIRAM
	/* Keep the large IQ ring out of scarce internal/DMA SRAM.  USB host
	 * transfer descriptors and DMA buffers must remain in internal memory. */
	const size_t iq_ring_bytes = (size_t)IQ_RING_SLOTS * IQ_CHUNK_SIZE;
	iq_ring = (uint8_t *)heap_caps_malloc(iq_ring_bytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
	if (iq_ring == NULL) {
		ESP_LOGE(TAG, "Failed to allocate IQ ring in PSRAM (%u bytes)", (unsigned)iq_ring_bytes);
		abort();
	}
	memset(iq_ring, 0, iq_ring_bytes);
	ESP_LOGI(TAG, "IQ ring allocated in PSRAM: %u bytes (%u slots x %u)",
	         (unsigned)iq_ring_bytes, (unsigned)IQ_RING_SLOTS, (unsigned)IQ_CHUNK_SIZE);
	ESP_LOGI(TAG, "After IQ ring alloc: free PSRAM=%u, free internal=%u",
	         (unsigned)heap_caps_get_free_size(MALLOC_CAP_SPIRAM),
	         (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
#else
	/* No PSRAM: allocate the smaller ring from normal heap. */
	const size_t iq_ring_bytes = (size_t)IQ_RING_SLOTS * IQ_CHUNK_SIZE;
	iq_ring = (uint8_t *)malloc(iq_ring_bytes);
	if (iq_ring == NULL) {
		ESP_LOGE(TAG, "Failed to allocate IQ ring (%u bytes)", (unsigned)iq_ring_bytes);
		abort();
	}
	memset(iq_ring, 0, iq_ring_bytes);
#endif

	usbhost_begin();
	vTaskDelay(1000 / portTICK_PERIOD_MS); 

    // Initialize GPIO
    //gpio_config_t io_conf = {
    //    .pin_bit_mask = (1ULL << LED_GPIO),
    //    .mode = GPIO_MODE_OUTPUT,
    //};
    //gpio_config(&io_conf);

    // Initialize Ethernet driver
    uint8_t eth_port_cnt = 0;
    esp_eth_handle_t *eth_handles;
    ESP_ERROR_CHECK(example_eth_init(&eth_handles, &eth_port_cnt));

    // Initialize TCP/IP network interface aka the esp-netif (should be called only once in application)
    ESP_ERROR_CHECK(esp_netif_init());
    // Create default event loop that running in background
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    esp_netif_t *eth_netifs[eth_port_cnt];
    esp_eth_netif_glue_handle_t eth_netif_glues[eth_port_cnt];

    // Create instance(s) of esp-netif for Ethernet(s)
    if (eth_port_cnt == 1) {
        // Use ESP_NETIF_DEFAULT_ETH when just one Ethernet interface is used and you don't need to modify
        // default esp-netif configuration parameters.
        /* Mobile mode: create Ethernet netif as DHCP SERVER, not DHCP client.
         * Put the static IPv4 configuration into the inherent config BEFORE
         * esp_netif_new().  This avoids stopping/restarting DHCP around
         * esp_netif_set_ip_info() and lets esp-netif start the DHCP server
         * at the normal interface-start event. */
        esp_netif_ip_info_t ip_info = {};
        IP4_ADDR(&ip_info.ip, MOBILE_ETH_IP_A, MOBILE_ETH_IP_B, MOBILE_ETH_IP_C, MOBILE_ETH_IP_D);
        IP4_ADDR(&ip_info.netmask, 255, 255, 255, 0);
        IP4_ADDR(&ip_info.gw, MOBILE_ETH_IP_A, MOBILE_ETH_IP_B, MOBILE_ETH_IP_C, MOBILE_ETH_IP_D);

        esp_netif_inherent_config_t base_cfg = ESP_NETIF_INHERENT_DEFAULT_ETH();
        base_cfg.flags = (esp_netif_flags_t)(
            (base_cfg.flags & (esp_netif_flags_t)~ESP_NETIF_DHCP_CLIENT) |
            ESP_NETIF_DHCP_SERVER);
        base_cfg.ip_info = &ip_info;

        esp_netif_config_t cfg = {};
        cfg.base = &base_cfg;
        cfg.stack = ESP_NETIF_NETSTACK_DEFAULT_ETH;
        eth_netifs[0] = esp_netif_new(&cfg);
        if (eth_netifs[0] == NULL) {
            ESP_LOGE(TAG, "Failed to create Ethernet netif");
            abort();
        }

        ESP_LOGI(TAG, "Mobile Ethernet configured: 192.168.50.1/24, DHCP server mode");

        eth_netif_glues[0] = esp_eth_new_netif_glue(eth_handles[0]);
        // Attach Ethernet driver to TCP/IP stack
        ESP_ERROR_CHECK(esp_netif_attach(eth_netifs[0], eth_netif_glues[0]));
    } else {
        // Use ESP_NETIF_INHERENT_DEFAULT_ETH when multiple Ethernet interfaces are used and so you need to modify
        // esp-netif configuration parameters for each interface (name, priority, etc.).
        esp_netif_inherent_config_t esp_netif_config = ESP_NETIF_INHERENT_DEFAULT_ETH();
        esp_netif_config_t cfg_spi = {};
	cfg_spi.base = &esp_netif_config;
	cfg_spi.stack = ESP_NETIF_NETSTACK_DEFAULT_ETH;

        char if_key_str[10];
        char if_desc_str[10];
        char num_str[3];
        for (int i = 0; i < eth_port_cnt; i++) {
            itoa(i, num_str, 10);
            strcat(strcpy(if_key_str, "ETH_"), num_str);
            strcat(strcpy(if_desc_str, "eth"), num_str);
            esp_netif_config.if_key = if_key_str;
            esp_netif_config.if_desc = if_desc_str;
            esp_netif_config.route_prio -= i*5;
            eth_netifs[i] = esp_netif_new(&cfg_spi);
            eth_netif_glues[i] = esp_eth_new_netif_glue(eth_handles[0]);
            // Attach Ethernet driver to TCP/IP stack
            ESP_ERROR_CHECK(esp_netif_attach(eth_netifs[i], eth_netif_glues[i]));
        }
    }

    // Register user defined event handers
    ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, &eth_event_handler,
                                               eth_port_cnt == 1 ? eth_netifs[0] : NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_ETH_GOT_IP, &got_ip_event_handler, NULL));

    // Start Ethernet driver state machine
    for (int i = 0; i < eth_port_cnt; i++) {
        ESP_ERROR_CHECK(esp_eth_start(eth_handles[i]));
    }

    print_memory_info();


	vTaskDelay(pdMS_TO_TICKS(3000));
	
	//dev_index = verbose_device_search(optarg);
	//dev_given = 1;
	frequency = 102400000;
	gain = 100;
	samp_rate = 240000;


	//if (!dev_given) {
	//	dev_index = verbose_device_search("0");
	//}

	//if (dev_index < 0) {
	//    while(1) delay(100);
	//	}
	int device_count, device, offset;
	char vendor[256], product[256], serial[256];  
	 device_count = rtlsdr_get_device_count();    
	if (!device_count) {
		fprintf(stderr, "No supported devices found.\n");
		while (1) { 
			vTaskDelay(1000 / portTICK_PERIOD_MS); 
		}
	}

  	fprintf(stderr, "Found %d device(s):\n", device_count);
	for (int i = 0; i < device_count; i++) {
		rtlsdr_get_device_usb_strings(i, vendor, product, serial);
		fprintf(stderr, "  %d:  %s, %s, SN: %s\n", i, vendor, product, serial);
	}


	dev_index = 0;

	rtlsdr_open(&dev, (uint32_t)dev_index);
	if (NULL == dev) {
	fprintf(stderr, "Failed to open rtlsdr device #%d.\n", dev_index);
		//exit(1);
		while (1) { 
			vTaskDelay(1000 / portTICK_PERIOD_MS); 
		}
	}
	//gpio_set_level(LED_GPIO, 1);

	/* Set direct sampling */
        if (direct_sampling)
                verbose_direct_sampling(dev, 2);

	/* Set the tuner error */
	verbose_ppm_set(dev, ppm_error);

	/* Set the sample rate */
	r = rtlsdr_set_sample_rate(dev, samp_rate);
	if (r < 0)
		fprintf(stderr, "WARNING: Failed to set sample rate.\n");

	/* Set the frequency */
	r = rtlsdr_set_center_freq(dev, frequency);
	if (r < 0)
		fprintf(stderr, "WARNING: Failed to set center freq.\n");
	else
		fprintf(stderr, "Tuned to %lu Hz.\n", (unsigned long)frequency);

	if (0 == gain) {
		 /* Enable automatic gain */
		r = rtlsdr_set_tuner_gain_mode(dev, 0);
		if (r < 0)
			fprintf(stderr, "WARNING: Failed to enable automatic gain.\n");
	} else {
		/* Enable manual gain */
		r = rtlsdr_set_tuner_gain_mode(dev, 1);
		if (r < 0)
			fprintf(stderr, "WARNING: Failed to enable manual gain.\n");

		/* Set the tuner gain */
		r = rtlsdr_set_tuner_gain(dev, gain);
		if (r < 0)
			fprintf(stderr, "WARNING: Failed to set tuner gain.\n");
		else
			fprintf(stderr, "Tuner gain set to %f dB.\n", gain/10.0);
	}

	rtlsdr_set_bias_tee(dev, enable_biastee);
	if (enable_biastee)
		fprintf(stderr, "activated bias-T on GPIO PIN 0\n");

	/* Reset endpoint before we start reading from it (mandatory) */
	r = rtlsdr_reset_buffer(dev);
	if (r < 0)
		fprintf(stderr, "WARNING: Failed to reset buffers.\n");

	pthread_mutex_init(&exit_cond_lock, NULL);
	pthread_mutex_init(&ll_mutex, NULL);
	pthread_mutex_init(&exit_cond_lock, NULL);
	pthread_cond_init(&cond, NULL);
	pthread_cond_init(&exit_cond, NULL);


///
int client_socket;
int ip_protocol;
int socket_id;
int bind_err;
int listen_error;
char addr_str[128];

	struct sockaddr_in destAddr;
	destAddr.sin_addr.s_addr = htonl(INADDR_ANY); //Change hostname to network byte order
	destAddr.sin_family = AF_INET;		//Define address family as Ipv4
	destAddr.sin_port = htons(netPort); 	//Define PORT
	int addr_family = AF_INET;				//Define address family as Ipv4
	ip_protocol = IPPROTO_TCP;			//Define protocol as TCP
	inet_ntoa_r(destAddr.sin_addr, addr_str, sizeof(addr_str) - 1);

	/* Create TCP socket*/
	listensocket = socket(addr_family, SOCK_STREAM, ip_protocol);
	if (listensocket < 0)
	{
		ESP_LOGE(TAG, "Unable to create socket: errno %d", errno);
	}
	ESP_LOGI(TAG, "Socket created");

	r = 1;
	setsockopt(listensocket, SOL_SOCKET, SO_REUSEADDR, (char *)&r, sizeof(int));
	setsockopt(listensocket, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling));

	/* Bind a socket to a specific IP + port */
	bind_err = bind(listensocket, (struct sockaddr *)&destAddr, sizeof(destAddr));
	if (bind_err != 0)
	{
		ESP_LOGE(TAG, "Socket unable to bind: errno %d", errno);
	}
	ESP_LOGI(TAG, "Socket bound");

///	

	r = fcntl(listensocket, F_GETFL, 0);
	r = fcntl(listensocket, F_SETFL, r | O_NONBLOCK);

	while(1) {
		printf("listening...\n");
		listen(listensocket,1);

		while(1) {
			FD_ZERO(&readfds);
			FD_SET(listensocket, &readfds);
			tv.tv_sec = 1;
			tv.tv_usec = 0;
			r = select(listensocket+1, &readfds, NULL, NULL, &tv);
			if(do_exit) {
				goto out;
			} else if(r) {
				rlen = sizeof(remote);
				s = accept(listensocket,(struct sockaddr *)&remote, &rlen);
				if (s >= 0) {
					int flags = fcntl(s, F_GETFL, 0);
					if (flags >= 0)
						fcntl(s, F_SETFL, flags & ~O_NONBLOCK);
				}
				break;
			}
		}

//		setsockopt(s, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling));

/*		getnameinfo((struct sockaddr *)&remote, rlen,
			    remhostinfo, NI_MAXHOST,
			    remportinfo, NI_MAXSERV, NI_NUMERICSERV);*/
		printf("client accepted!\n"); // %s %s\n", remhostinfo, remportinfo);


		memset(&dongle_info, 0, sizeof(dongle_info));
		memcpy(&dongle_info.magic, "RTL0", 4);

		r = rtlsdr_get_tuner_type(dev);
		if (r >= 0)
			dongle_info.tuner_type = htonl(r);

		r = rtlsdr_get_tuner_gains(dev, NULL);
		if (r >= 0)
			dongle_info.tuner_gain_count = htonl(r);

		r = send(s, (const char *)&dongle_info, sizeof(dongle_info), 0);
		if (sizeof(dongle_info) != r)
			printf("failed to send dongle information\n");

		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
		r = pthread_create(&tcp_worker_thread, &attr, tcp_worker, NULL);
		r = pthread_create(&command_thread, &attr, command_worker, NULL);
		pthread_attr_destroy(&attr);

		r = rtlsdr_read_async(dev, rtlsdr_callback, NULL, buf_num, 5120);

		pthread_join(tcp_worker_thread, &status);
		pthread_join(command_thread, &status);

		closesocket(s);

		printf("all threads dead..\n");
		pthread_mutex_lock(&ll_mutex);
		iq_ring_head = 0;
		iq_ring_tail = 0;
		iq_ring_count = 0;
		iq_build_len = 0;
		memset(iq_ring_len, 0, sizeof(iq_ring_len));
		pthread_mutex_unlock(&ll_mutex);

		do_exit = 0;
		global_numq = 0;
		iq_dropped_blocks = 0;
		iq_completed_chunks = 0;
	}

out:
	rtlsdr_close(dev);
	//gpio_set_level(LED_GPIO, 0);	
	closesocket(listensocket);
	closesocket(s);

	printf("bye!\n");
	while (1) { 
		vTaskDelay(1000 / portTICK_PERIOD_MS); 
	}
}
