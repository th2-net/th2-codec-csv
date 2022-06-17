# Csv codec (3.2.1)
## Description
Designed for decode csv raw messages from csv reader to the parsed messages.

## Decoding

The codec decodes each raw message in the received batch.
Each raw message might contains several line in CSV format.
If the default header parameter is not set the codec trites the first line from the raw message as a header.
Otherwise, the default header will be used for decoding the rest of data.

If no data was decoded from raw message, the message will be skipped, and an error event will be reported.

## Decode Example

Simple example: 

```text
A, B, V, G
1, 2, 3, 4
```

into

```json
{
  "A": 1,
  "B": 2,
  "V": 3,
  "G": 6
}
```

***

**Array** example:

```text
A, B, V,  ,  , G, D
1, 2, 3, 4, 5, 6, 7
```

into

```json
{
  "A": 1,
  "B": 2,
  "V": [3, 4, 5],
  "G": 6,
  "D": 7
}
```

## Settings
Csv codec has following parameters:

```yaml
default-header: [A, B, C]
delimiter: ','
encoding: UTF-8
display-name: CodecCsv
validate-length: true
```
**default-header** - the default header for this codec. It will be used if no header found in the received batch.

**delimiter** - the delimiter to split values in received data. The default value is `,`.

**encoding** - the encoding for the received data. The default value is `UTF-8`.

**display-name** - the name to set in the root event sent to the event store. All errors during decoding will be attached to that root event.
The default value for the name is `CodecCsv`.

**validate-length** - check if csv have different count of values against header's count

## Pins

The CSV codec requires at leas one pin with the following attributes:
1. `decode_in` and `subscribe` - to receive raw data.
2. `decode_out` and `publish` - to send decoded data.

The number of pins with each set of attributes is not limited. **Pins can use filters**.

## Full configuration example

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2GenericBox
metadata:
  name: codec-csv
spec:
  custom-config:
    default-header: [A, B, C]
    delimiter: ','
    encoding: UTF-8
  pins:
    # decoder
    - name: in_codec_decode
      connection-type: mq
      attributes: ['decode_in', 'subscribe', 'group']
    - name: out_codec_decode
      connection-type: mq
      attributes: ['decode_out', 'publish', 'parsed']
```

## Release notes

### 3.1.1

+ fixed: last array-field as simple value

### 3.1.0

+ new length validation parameter
+ array support, to specify array need to have empty header for each value [^array]

### 3.1.0

+ reads dictionaries from the /var/th2/config/dictionary folder.
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration
+ update Cradle version. Introduce async API for storing events
+ removed gRPC event loop handling
+ fixed dictionary reading
