#![no_std]
#![no_main]

use core::fmt::Write;
use core::mem::MaybeUninit;

use dht_sensor::{dht22, DhtReading};
use embassy_executor::Spawner;
use embassy_net::tcp::TcpSocket;
use embassy_net::{Config, IpAddress, StackResources};
use embassy_time::{with_timeout, Duration, Instant, Timer};
use esp_alloc as _;
use esp_backtrace as _;
use esp_hal::delay::Delay;
use esp_hal::gpio::{Level, Output, OutputOpenDrain, Pull};
use esp_hal::i2c::master::{Config as I2cConfig, I2c};
use esp_hal::rng::Rng;
use esp_hal::timer::timg::TimerGroup;
use esp_hal::uart::Uart;
use esp_hal::Config as HalConfig;
use esp_hal_embassy::main;
use esp_println::println;
use esp_wifi::wifi::{ClientConfiguration, WifiController, WifiDevice, WifiStaDevice};
use esp_wifi::EspWifiController;
use heapless::String;
use embedded_tls::{Aes128GcmSha256, NoVerify, TlsConfig, TlsConnection, TlsContext};
use tinymqtt::MqttClient;

extern crate alloc;

// =========================================================
// ALOKASI MEMORI DENGAN HEAP DINAMIS
// =========================================================
const HEAP_SIZE: usize = 128 * 1024;
static mut HEAP: MaybeUninit<[u8; HEAP_SIZE]> = MaybeUninit::uninit();

#[no_mangle]
pub extern "C" fn esp_wifi_allocate_from_internal_ram(size: usize) -> *mut u8 {
    unsafe {
        let layout = core::alloc::Layout::from_size_align_unchecked(size, 4);
        alloc::alloc::alloc(layout)
    }
}

#[no_mangle]
pub extern "C" fn esp_wifi_free_internal_heap(ptr: *mut u8) {
    unsafe {
        if !ptr.is_null() {
            let layout = core::alloc::Layout::from_size_align_unchecked(1, 4);
            alloc::alloc::dealloc(ptr, layout);
        }
    }
}

fn init_heap() {
    unsafe {
        esp_alloc::HEAP.add_region(esp_alloc::HeapRegion::new(
            core::ptr::addr_of_mut!(HEAP).cast::<u8>(),
            HEAP_SIZE,
            esp_alloc::MemoryCapability::Internal.into(),
        ));
    }
}

// =========================================================
// KONFIGURASI MQTT & SENSOR
// =========================================================
const DEFAULT_DEVICE_ID: &str = "ESP32-001";
const DEFAULT_INTERVAL_SECONDS: u32 = 5;
const MAX_SAMPLE_ID: u32 = 18;
const MQTT_TOPIC: &str = "enose/ESP32-001/measurement";
const MQTT_HOST: &str = "broker.emqx.io";
const MQTT_PORT: u16 = 8883;

const ADS1_ADDR: u8 = 0x48;
const ADS2_ADDR: u8 = 0x49;
const ADS_REG_CONVERT: u8 = 0x00;
const ADS_REG_CONFIG: u8 = 0x01;

static NET_RESOURCES: static_cell::StaticCell<StackResources<4>> = static_cell::StaticCell::new();
static WIFI_INIT: static_cell::StaticCell<EspWifiController<'static>> =
    static_cell::StaticCell::new();

// =========================================================
// BUFFER JARINGAN & TLS (static supaya tidak membebani stack/arena task)
// =========================================================
static mut TCP_RX_BUF: [u8; 4096] = [0; 4096];
static mut TCP_TX_BUF: [u8; 4096] = [0; 4096];
static mut TLS_READ_BUF: [u8; 16640] = [0; 16640]; // ukuran maksimum record TLS
static mut TLS_WRITE_BUF: [u8; 4096] = [0; 4096];

// =========================================================
// RNG UNTUK TLS (esp-hal Rng + marker CryptoRng)
// Saat Wi-Fi aktif, RNG hardware ESP32 memakai noise radio sebagai sumber entropi.
// =========================================================
struct TlsRng(Rng);

impl rand_core::RngCore for TlsRng {
    fn next_u32(&mut self) -> u32 { rand_core::RngCore::next_u32(&mut self.0) }
    fn next_u64(&mut self) -> u64 { rand_core::RngCore::next_u64(&mut self.0) }
    fn fill_bytes(&mut self, dest: &mut [u8]) { rand_core::RngCore::fill_bytes(&mut self.0, dest) }
    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
        rand_core::RngCore::try_fill_bytes(&mut self.0, dest)
    }
}
impl rand_core::CryptoRng for TlsRng {}

/// Kirim data lewat TLS (tulis semua byte lalu flush). true = sukses.
async fn tls_send(
    tls: &mut TlsConnection<'_, TcpSocket<'_>, Aes128GcmSha256>,
    data: &[u8],
) -> bool {
    let mut sent = 0;
    while sent < data.len() {
        match tls.write(&data[sent..]).await {
            Ok(0) | Err(_) => return false,
            Ok(n) => sent += n,
        }
    }
    tls.flush().await.is_ok()
}

// =========================================================
// STRUKTUR DATA & STATE BERSAMA (Zero-Dependency)
// =========================================================
#[derive(Clone, Copy, Default)]
pub struct SensorData {
    pub mq6: f32, pub mq135: f32, pub mq3: f32, pub mq7: f32,
    pub tgs2611: f32, pub tgs2602: f32, pub tgs2600: f32, pub tgs2620: f32,
    pub dht1_temp: f32, pub dht1_hum: f32, pub dht2_temp: f32, pub dht2_hum: f32,
}

static mut SHARED_SENSOR: SensorData = SensorData {
    mq6: 0.0, mq135: 0.0, mq3: 0.0, mq7: 0.0,
    tgs2611: 0.0, tgs2602: 0.0, tgs2600: 0.0, tgs2620: 0.0,
    dht1_temp: 0.0, dht1_hum: 0.0, dht2_temp: 0.0, dht2_hum: 0.0,
};

// =========================================================
// WRAPPER EMBEDDED-HAL 0.2 (UNTUK SENSOR DHT22)
// =========================================================
struct OldDelay(Delay);

impl embedded_hal::blocking::delay::DelayUs<u8> for OldDelay {
    fn delay_us(&mut self, us: u8) { self.0.delay_micros(us as u32); }
}
impl embedded_hal::blocking::delay::DelayUs<u16> for OldDelay {
    fn delay_us(&mut self, us: u16) { self.0.delay_micros(us as u32); }
}
impl embedded_hal::blocking::delay::DelayUs<u32> for OldDelay {
    fn delay_us(&mut self, us: u32) { self.0.delay_micros(us); }
}
impl embedded_hal::blocking::delay::DelayMs<u8> for OldDelay {
    fn delay_ms(&mut self, ms: u8) { self.0.delay_millis(ms as u32); }
}
impl embedded_hal::blocking::delay::DelayMs<u16> for OldDelay {
    fn delay_ms(&mut self, ms: u16) { self.0.delay_millis(ms as u32); }
}
impl embedded_hal::blocking::delay::DelayMs<u32> for OldDelay {
    fn delay_ms(&mut self, ms: u32) { self.0.delay_millis(ms); }
}

struct OldPin<'a>(OutputOpenDrain<'a>);

impl<'a> embedded_hal::digital::v2::InputPin for OldPin<'a> {
    type Error = core::convert::Infallible;
    fn is_high(&self) -> Result<bool, Self::Error> { Ok(self.0.is_high()) }
    fn is_low(&self) -> Result<bool, Self::Error> { Ok(self.0.is_low()) }
}
impl<'a> embedded_hal::digital::v2::OutputPin for OldPin<'a> {
    type Error = core::convert::Infallible;
    fn set_high(&mut self) -> Result<(), Self::Error> { self.0.set_high(); Ok(()) }
    fn set_low(&mut self) -> Result<(), Self::Error> { self.0.set_low(); Ok(()) }
}

// =========================================================
// DRIVER ACTUATOR MACROS
// =========================================================
macro_rules! driver_start {
    ($in1:expr, $in2:expr, $in3:expr, $in4:expr) => {{
        $in1.set_high(); $in2.set_low();
        $in3.set_high(); $in4.set_low();
    }};
}

macro_rules! driver_stop {
    ($in1:expr, $in2:expr, $in3:expr, $in4:expr) => {{
        $in1.set_low(); $in2.set_low();
        $in3.set_low(); $in4.set_low();
    }};
}

// =========================================================
// FUNGSI BACA ADS1115 (I2C)
// =========================================================
fn ads_config(channel: u8) -> [u8; 3] {
    let mux: u16 = match channel {
        0 => 0b100, 1 => 0b101, 2 => 0b110, 3 => 0b111, _ => 0b100,
    };
    let high: u8 = (0b1_000_000_1u8) | ((mux as u8) << 4);
    let low: u8 = 0b1000_0011;
    [ADS_REG_CONFIG, high, low]
}

fn ads_read_single(i2c: &mut I2c<'_, esp_hal::Blocking>, addr: u8, channel: u8, delay: &Delay) -> Result<i16, esp_hal::i2c::master::Error> {
    let cfg = ads_config(channel);
    i2c.write(addr, &cfg)?;
    delay.delay_millis(10);
    i2c.write(addr, &[ADS_REG_CONVERT])?;
    let mut buf = [0u8; 2];
    i2c.read(addr, &mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

fn raw_to_mv(raw: i16) -> f32 {
    (raw as f32) * 6144.0 / 32767.0
}

// =========================================================
// FUNGSI ENKODE PACKET MQTT
// =========================================================
fn write_mqtt_publish<'a>(buffer: &'a mut [u8], topic: &str, payload: &[u8]) -> Option<&'a [u8]> {
    let remaining_length = 2usize.checked_add(topic.len())?.checked_add(1)?.checked_add(payload.len())?;
    if topic.len() > u16::MAX as usize || remaining_length > 268_435_455 { return None; }

    let mut cursor = 0;
    buffer[cursor] = 0x30;
    cursor += 1;

    let mut encoded_length = remaining_length as u32;
    loop {
        let mut byte = (encoded_length % 128) as u8;
        encoded_length /= 128;
        if encoded_length > 0 { byte |= 0x80; }
        if cursor >= buffer.len() { return None; }
        buffer[cursor] = byte;
        cursor += 1;
        if encoded_length == 0 { break; }
    }

    let end = cursor.checked_add(2)?.checked_add(topic.len())?.checked_add(1)?.checked_add(payload.len())?;
    if end > buffer.len() { return None; }

    buffer[cursor..cursor + 2].copy_from_slice(&(topic.len() as u16).to_be_bytes());
    cursor += 2;
    buffer[cursor..cursor + topic.len()].copy_from_slice(topic.as_bytes());
    cursor += topic.len();
    buffer[cursor] = 0;
    cursor += 1;
    buffer[cursor..end].copy_from_slice(payload);

    Some(&buffer[..end])
}

// =========================================================
// TASKS JARINGAN & MQTT
// =========================================================
#[embassy_executor::task]
async fn net_task(stack: &'static embassy_net::Stack<WifiDevice<'static, WifiStaDevice>>) -> ! {
    stack.run().await
}

#[embassy_executor::task]
async fn mqtt_task(
    stack: &'static embassy_net::Stack<WifiDevice<'static, WifiStaDevice>>,
    mut wifi_controller: WifiController<'static>,
    rng: Rng,
) -> ! {
    println!(">>> Network task started");
    wifi_controller.start().unwrap();
    loop {
        match wifi_controller.connect_async().await {
            Ok(()) => { println!("wifi: connected"); break; }
            Err(_) => {
                println!("wifi: retry connect...");
                Timer::after(Duration::from_secs(3)).await;
            }
        }
    }

    stack.wait_config_up().await;
    println!("dhcp ready");

    let addresses = match stack.dns_query(MQTT_HOST, embassy_net::dns::DnsQueryType::A).await {
        Ok(addr) => addr,
        Err(_) => loop { Timer::after(Duration::from_secs(3)).await; },
    };

    let broker_ip = match addresses.first() {
        Some(IpAddress::Ipv4(address)) => IpAddress::Ipv4(address.clone()),
        _ => loop { Timer::after(Duration::from_secs(3)).await; },
    };

    // Buffer static (dipakai ulang di setiap sesi koneksi)
    let tcp_rx: &'static mut [u8; 4096] = unsafe { &mut *core::ptr::addr_of_mut!(TCP_RX_BUF) };
    let tcp_tx: &'static mut [u8; 4096] = unsafe { &mut *core::ptr::addr_of_mut!(TCP_TX_BUF) };
    let tls_read: &'static mut [u8; 16640] = unsafe { &mut *core::ptr::addr_of_mut!(TLS_READ_BUF) };
    let tls_write: &'static mut [u8; 4096] = unsafe { &mut *core::ptr::addr_of_mut!(TLS_WRITE_BUF) };

    let mut tls_rng = TlsRng(rng);
    let mut packet_buffer = [0u8; 512];
    let mut publish_buffer = [0u8; 1024];
    let mut sample_id = 1u32;

    loop {
        // ---------- 1. TCP ----------
        let mut socket = TcpSocket::new(stack, &mut tcp_rx[..], &mut tcp_tx[..]);
        socket.set_timeout(Some(Duration::from_secs(30)));

        if socket.connect((broker_ip, MQTT_PORT)).await.is_err() {
            println!("MQTT TCP connect failed, retry in 5s");
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        println!("MQTT TCP connected, TLS handshake...");

        // ---------- 2. TLS ----------
        // NoVerify: sertifikat server belum diverifikasi (enkripsi aktif, tapi tanpa
        // proteksi terhadap server palsu). Nanti bisa diganti dengan verifikasi CA.
        let config = TlsConfig::<Aes128GcmSha256>::new()
            .with_server_name(MQTT_HOST)
            .enable_rsa_signatures();
        let mut tls: TlsConnection<'_, TcpSocket<'_>, Aes128GcmSha256> =
            TlsConnection::new(socket, &mut tls_read[..], &mut tls_write[..]);

        if let Err(e) = tls
            .open::<TlsRng, NoVerify>(TlsContext::new(&config, &mut tls_rng))
            .await
        {
            println!("TLS handshake failed: {:?}", e);
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        println!("TLS handshake OK");

        // ---------- 3. MQTT CONNECT (di dalam TLS) ----------
        let mut client: MqttClient<1024> = MqttClient::new();
        // Set None karena broker publik EMQX tidak butuh autentikasi
        let credentials = None; 
        let sent = match client.connect(DEFAULT_DEVICE_ID, credentials) {
            Ok(packet) => tls_send(&mut tls, packet).await,
            Err(_) => false,
        };
        if !sent {
            println!("MQTT CONNECT send failed");
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }

        let response_length =
            match with_timeout(Duration::from_secs(10), tls.read(&mut packet_buffer)).await {
                Ok(Ok(len)) => len,
                _ => 0,
            };
        if response_length == 0
            || client
                .receive_packet(&packet_buffer[..response_length], |_, _, _| {})
                .is_err()
        {
            println!("MQTT CONNACK failed (cek username/password & izin di HiveMQ)");
            Timer::after(Duration::from_secs(5)).await;
            continue;
        }
        println!("MQTT connected!");

        // ---------- 4. PUBLISH LOOP ----------
        loop {
            // Ambil data real-time sensor
            let sensor = unsafe { SHARED_SENSOR };

            let mut payload: String<384> = String::new();
            let _ = write!(
                payload,
                "{{\"sample_id\":{},\"score\":{:.1},\"accuracy\":{:.1},\"source\":\"enose\",\"device_id\":\"{}\",\"features\":[{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0}]}}",
                sample_id,
                sensor.dht1_temp,
                sensor.dht1_hum,
                DEFAULT_DEVICE_ID,
                sensor.mq6, sensor.mq135, sensor.mq3, sensor.mq7,
                sensor.tgs2611, sensor.tgs2602, sensor.tgs2600, sensor.tgs2620
            );

            if let Some(packet) =
                write_mqtt_publish(&mut publish_buffer, MQTT_TOPIC, payload.as_bytes())
            {
                if tls_send(&mut tls, packet).await {
                    println!(">>> SUKSES PUBLISH: sample {} -> {}", sample_id, payload.as_str());
                } else {
                    println!("MQTT publish failed, reconnecting...");
                    break; // keluar ke loop luar: TCP + TLS + MQTT dibuat ulang
                }
            }

            sample_id = if sample_id >= MAX_SAMPLE_ID { 1 } else { sample_id + 1 };
            Timer::after(Duration::from_secs(DEFAULT_INTERVAL_SECONDS as u64)).await;
        }

        Timer::after(Duration::from_secs(3)).await;
    }
}

// =========================================================
// MAIN ENTRY POINT
// =========================================================
#[main]
async fn main(spawner: Spawner) -> ! {
    println!("boot: starting E-Nose application");
    init_heap();

    let peripherals = esp_hal::init(HalConfig::default());
    let delay = Delay::new();

    // 1. PIN UART0 (TX = GPIO43, RX = GPIO44)
    let _uart = Uart::new(peripherals.UART0, peripherals.GPIO43, peripherals.GPIO44)
        .expect("Failed to initialize UART0");

    // 2. PIN I2C (SDA = GPIO8, SCL = GPIO9)
    let mut i2c = I2c::new(peripherals.I2C0, I2cConfig::default())
        .with_sda(peripherals.GPIO8)
        .with_scl(peripherals.GPIO9);

    // 3. PIN DRIVER A - INLET (GPIO5, GPIO17, GPIO14, GPIO19)
    let mut in1_a = Output::new(peripherals.GPIO5, Level::Low);
    let mut in2_a = Output::new(peripherals.GPIO17, Level::Low);
    let mut in3_a = Output::new(peripherals.GPIO14, Level::Low);
    let mut in4_a = Output::new(peripherals.GPIO1, Level::Low); // sementara, tes tanpa pompa

    // 4. PIN DRIVER B - OUTLET (GPIO16, GPIO4, GPIO45, GPIO20)
    let mut in1_b = Output::new(peripherals.GPIO16, Level::Low);
    let mut in2_b = Output::new(peripherals.GPIO4, Level::Low);
    let mut in3_b = Output::new(peripherals.GPIO45, Level::Low);
    let mut in4_b = Output::new(peripherals.GPIO2, Level::Low); // sementara, tes tanpa pompa

    driver_stop!(in1_a, in2_a, in3_a, in4_a);
    driver_stop!(in1_b, in2_b, in3_b, in4_b);

    // 5. PIN SENSOR DHT22 (Sensor 1 = GPIO10, Sensor 2 = GPIO11) menggunakan OutputOpenDrain
    let dht1_io = OutputOpenDrain::new(peripherals.GPIO10, Level::High, Pull::Up);
    let mut dht1_pin = OldPin(dht1_io);

    let dht2_io = OutputOpenDrain::new(peripherals.GPIO11, Level::High, Pull::Up);
    let mut dht2_pin = OldPin(dht2_io);

    // POST: Uji Pompa 3 Detik di awal boot
    println!(">>> Running Power-On Self-Test (3s)...");
    driver_start!(in1_a, in2_a, in3_a, in4_a);
    driver_start!(in1_b, in2_b, in3_b, in4_b);
    delay.delay_millis(3000);
    driver_stop!(in1_a, in2_a, in3_a, in4_a);
    driver_stop!(in1_b, in2_b, in3_b, in4_b);
    println!(">>> POST Selesai.");

    // Inisialisasi Wi-Fi & Embassy Timers
    let embassy_timers = TimerGroup::new(peripherals.TIMG1);
    esp_hal_embassy::init(embassy_timers.timer0);

    let wifi_timers = TimerGroup::new(peripherals.TIMG0);
    let rng = Rng::new(peripherals.RNG); // Rng bersifat Copy: dipakai Wi-Fi dan TLS
    let wifi_init = WIFI_INIT.init(
        esp_wifi::init(wifi_timers.timer0, rng, peripherals.RADIO_CLK).unwrap(),
    );

    let wifi_config = ClientConfiguration {
        ssid: "Ujicoba".try_into().unwrap(),
        password: "amandaaa".try_into().unwrap(),
        ..Default::default()
    };
    let (wifi_device, wifi_controller) =
        esp_wifi::wifi::new_with_config(wifi_init, peripherals.WIFI, wifi_config).unwrap();

    static STACK: static_cell::StaticCell<embassy_net::Stack<WifiDevice<'static, WifiStaDevice>>> =
        static_cell::StaticCell::new();
    let stack = STACK.init(embassy_net::Stack::new(
        wifi_device,
        Config::dhcpv4(Default::default()),
        NET_RESOURCES.init(StackResources::new()),
        0x1234_5678,
    ));

    // Jalankan background networking & MQTT
    spawner.spawn(net_task(stack)).unwrap();
    spawner.spawn(mqtt_task(stack, wifi_controller, rng)).unwrap();

    let mut elapsed_ticks = 0u64;
    let running = true;
    let mut last_read_ms = Instant::now().as_millis();

    // LOOP UTAMA (KONTROL SIKLUS E-NOSE & BACA SENSOR)
    loop {
        let now = Instant::now().as_millis();

        if now.saturating_sub(last_read_ms) >= 1000 {
            if running {
                elapsed_ticks += 1;
            }

            let mut old_delay = OldDelay(Delay::new());
            let mut temp1 = 0.0;
            let mut hum1 = 0.0;
            let mut temp2 = 0.0;
            let mut hum2 = 0.0;

            if let Ok(reading) = dht22::Reading::read(&mut old_delay, &mut dht1_pin) {
                temp1 = reading.temperature;
                hum1 = reading.relative_humidity;
            }
            if let Ok(reading) = dht22::Reading::read(&mut old_delay, &mut dht2_pin) {
                temp2 = reading.temperature;
                hum2 = reading.relative_humidity;
            }

            // Baca 8 Sensor Gas dari 2 ADS1115
            let mq6     = raw_to_mv(ads_read_single(&mut i2c, ADS1_ADDR, 0, &delay).unwrap_or(0));
            let mq135   = raw_to_mv(ads_read_single(&mut i2c, ADS1_ADDR, 1, &delay).unwrap_or(0));
            let mq3     = raw_to_mv(ads_read_single(&mut i2c, ADS1_ADDR, 2, &delay).unwrap_or(0));
            let mq7     = raw_to_mv(ads_read_single(&mut i2c, ADS1_ADDR, 3, &delay).unwrap_or(0));
            let tgs2611 = raw_to_mv(ads_read_single(&mut i2c, ADS2_ADDR, 0, &delay).unwrap_or(0));
            let tgs2602 = raw_to_mv(ads_read_single(&mut i2c, ADS2_ADDR, 1, &delay).unwrap_or(0));
            let tgs2600 = raw_to_mv(ads_read_single(&mut i2c, ADS2_ADDR, 2, &delay).unwrap_or(0));
            let tgs2620 = raw_to_mv(ads_read_single(&mut i2c, ADS2_ADDR, 3, &delay).unwrap_or(0));

            // Simpan ke shared memory untuk task MQTT
            unsafe {
                SHARED_SENSOR = SensorData {
                    mq6, mq135, mq3, mq7,
                    tgs2611, tgs2602, tgs2600, tgs2620,
                    dht1_temp: temp1, dht1_hum: hum1,
                    dht2_temp: temp2, dht2_hum: hum2,
                };
            }

            // Siklus Katup dan Pompa E-Nose
            if running {
                if elapsed_ticks >= 1 && elapsed_ticks <= 70 {
                    driver_stop!(in1_a, in2_a, in3_a, in4_a);
                    driver_stop!(in1_b, in2_b, in3_b, in4_b);
                } else if elapsed_ticks >= 71 && elapsed_ticks <= 170 {
                    driver_start!(in1_a, in2_a, in3_a, in4_a);
                    driver_stop!(in1_b, in2_b, in3_b, in4_b);
                } else if elapsed_ticks >= 171 && elapsed_ticks <= 230 {
                    driver_stop!(in1_a, in2_a, in3_a, in4_a);
                    driver_stop!(in1_b, in2_b, in3_b, in4_b);
                } else if elapsed_ticks >= 231 && elapsed_ticks <= 360 {
                    driver_stop!(in1_a, in2_a, in3_a, in4_a);
                    driver_start!(in1_b, in2_b, in3_b, in4_b);
                } else if elapsed_ticks > 360 {
                    elapsed_ticks = 0;
                    driver_stop!(in1_a, in2_a, in3_a, in4_a);
                    driver_stop!(in1_b, in2_b, in3_b, in4_b);
                }
            }

            last_read_ms = now;
        }

        // Delay non-blocking Embassy
        Timer::after(Duration::from_millis(50)).await;
    }
}