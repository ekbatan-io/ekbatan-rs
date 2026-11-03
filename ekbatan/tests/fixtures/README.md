# Fixtures

Recorded from Java Ekbatan's default Jackson 3 mapper, by the ekbatan-py parity probe
(`ekbatan-py/parity-probes/G-domain-model/`, `JsonFormatsParityProbeTest.java` and
`JsonParityProbeTest.java`), and copied here unchanged except for trimming:

- `java_formats.txt`: `kind<TAB>input<TAB>what Jackson wrote`, one value per line.
  - `dbl`: a double, by its IEEE bits in hex (6000 of the probe's 20009).
  - `bd`: a BigDecimal as `unscaled,scale` (only those `rust_decimal` can hold: scale 0..=28,
    under 2^96).
  - `instant`: an Instant as `epochSeconds,micros` (years 1..9999).
- `java_event.json`: one event, every kind of field, as Jackson wrote it.
