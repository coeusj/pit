# Car Sensors

Send sensors data coming from a car and push them into a queue in RabbitMQ

## Run car simulator

```bash
cargo run --bin car
```

## Run data receiver

```bash
cargo run --bin pit
```

Settings configuration: `./Settings.toml`

Available configurations:
- UDP
- RabbitMQ
- Sensors

To add new sensors:
```toml
[[sensors]]
id = 'sensor-id'
frequency_hz = 'update-frequency'
```