# Routing a custom MAVLink dialect

RMR ships with `common.xml` + `ardupilotmega.xml` compiled in. Unknown
msgids still forward — as broadcast, with the CRC unchecked. **RMR
never rejects a frame for being unknown.** Fork only if you need one
of these on your custom msgids:

- **Targeted routing** — dispatch by `(target_system, target_component)`
  instead of broadcasting to every endpoint.
- **CRC validation at ingress** — drop corrupt frames at the source and
  count them in `crc_errors` instead of forwarding them downstream.

Otherwise use the upstream binaries unmodified.

## Recipe

1. Fork [https://github.com/sheijningen/mavlink-router-rs](https://github.com/sheijningen/mavlink-router-rs)
   and clone your fork.
2. Copy your dialect XML (plus anything it `<include>`s) into
   [`vendor/mavlink/`](.).
3. Append the filename to `DIALECTS` in [`build.rs`](../../build.rs):
   ```rust
   const DIALECTS: &[&str] = &["common.xml", "ardupilotmega.xml", "myco.xml"];
   ```
4. `cargo build --release`.

A `crc_extra` collision with a built-in
entry is a fatal build error (never a silent runtime override) —
rename, pick a different msgid, or drop the colliding message.

## Worked example

Drop this into `vendor/mavlink/example_custom.xml`:

```xml
<?xml version="1.0"?>
<mavlink>
  <version>1</version>
  <dialect>9</dialect>
  <messages>
    <message id="60000" name="EXAMPLE_TARGETED">
      <field type="uint8_t" name="target_system">Recipient sysid.</field>
      <field type="uint8_t" name="target_component">Recipient compid.</field>
      <field type="uint32_t" name="command_arg">Opaque argument.</field>
    </message>
    <message id="60001" name="EXAMPLE_BROADCAST_STATUS">
      <field type="uint16_t" name="health">Subsystem health bits.</field>
      <field type="uint8_t" name="battery_pct">Battery percent.</field>
    </message>
  </messages>
</mavlink>
```

`id` values are in the [MAVLink user-test range](https://mavlink.io/en/guide/define_xml_element.html#message_id_ranges)
to avoid collisions with the vendored set (`common.xml` +
`ardupilotmega.xml` + their `<include>` graph).

Add `"example_custom.xml"` to `DIALECTS`, run `cargo build --release`,
then verify the generated table:

```sh
find target -name generated_msgid_table.rs -exec grep -E '60000|60001' {} +
```

Expected:

```rust
(60000, MsgEntry { crc_extra: 36, target_sys_offset: Some(4), target_comp_offset: Some(5) }),
(60001, MsgEntry { crc_extra: 240, target_sys_offset: None, target_comp_offset: None }),
```

The `Some(_)` offsets on 60000 confirm targeted dispatch is wired up;
60001's `None`/`None` is correct for a message with no targeting
fields.
