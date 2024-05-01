# Csv codec (5.3.0)

## Description

Designed for decode csv raw messages from csv reader to the parsed messages. It is based
on [th2-codec](https://github.com/th2-net/th2-codec). You can find additional
information [here](https://github.com/th2-net/th2-codec/blob/master/README.md)

## Decoding

The codec decodes each raw message in the received batch. Each raw message might contain several line in CSV format. If
the default header parameter is not set the codec trites the first line from the raw message as a header. Otherwise, the
default header will be used for decoding the rest of data. Output message type is taken
from `th2.csv.override_message_type` property in input message. If the property missing, the default
value (`Csv_Message`) for output message type is used.

If no data was decoded from raw message, the message will be skipped, and an error event will be reported.

**NOTE: the encoding is not supported**.

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
  "V": [
    3,
    4,
    5
  ],
  "G": 6,
  "D": 7
}
```

## Settings

Csv codec has the following parameters:

```yaml
default-header: [ A, B, C ]
delimiter: ','
encoding: UTF-8
display-name: CodecCsv
validate-length: true
publish-header: false
trim-whitespace: true
```

**default-header** - the default header for this codec. It will be used if no header found in the received batch.
  codec-csv trims all values in `default-header` and executes blank check. 

**delimiter** - the delimiter to split values in received data. The default value is `,`.

**encoding** - the encoding for the received data. The default value is `UTF-8`.

**display-name** - the name to set in the root event sent to the event store. All errors during decoding will be
attached to that root event. The default value for the name is `CodecCsv`.

**validate-length** - check if csv have different count of values against header's count.

**publish-header** - set to publish decoded header. The default value is `false`.

**trim-whitespace** - set to trim whitespace in header and cell. The default value is `true`.

## Full configuration example

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec-csv
spec:
  image-name: ghcr.io/th2-net/th2-codec-csv
  image-version: 4.0.0
  custom-config:
    codecSettings:
      default-header: [ A, B, C ]
      delimiter: ','
      encoding: UTF-8
  pins:
    # encoder
    - name: in_codec_encode
      connection-type: mq
      attributes: [ 'encoder_in', 'parsed', 'subscribe' ]
    - name: out_codec_encode
      connection-type: mq
      attributes: [ 'encoder_out', 'raw', 'publish' ]
    # decoder
    - name: in_codec_decode
      connection-type: mq
      attributes: [ 'decoder_in', 'raw', 'subscribe' ]
    - name: out_codec_decode
      connection-type: mq
      attributes: [ 'decoder_out', 'parsed', 'publish' ]
    # encoder general (technical)
    - name: in_codec_general_encode
      connection-type: mq
      attributes: [ 'general_encoder_in', 'parsed', 'subscribe' ]
    - name: out_codec_general_encode
      connection-type: mq
      attributes: [ 'general_encoder_out', 'raw', 'publish' ]
    # decoder general (technical)
    - name: in_codec_general_decode
      connection-type: mq
      attributes: [ 'general_decoder_in', 'raw', 'subscribe' ]
    - name: out_codec_general_decode
      connection-type: mq
      attributes: [ 'general_decoder_out', 'parsed', 'publish' ]
```

## Release notes

### 5.3.0
+ Migrated to th2 gradle plugin: `0.0.6`
+ Updated:
  + bom `4.6.1`
  + common: `5.10.1-dev`
  + common-utils: `2.2.3-dev`
  + codec: `5.5.0-dev`

### 5.2.1

+ Updated common: `5.7.2-dev`
+ Updated codec: `5.4.1-dev`

### 5.2.0

+ Added `trim-whitespace` option.
+ Updated common:5.7.1-dev
+ Updated common-utils:2.2.2-dev
+ Updated codec:5.4.0-dev

### 5.1.0

+ Supports th2 transport protocol
+ Updated bom:4.4.0
+ Updated common:5.3.0
+ Updated codec:5.3.0

### 5.0.0

+ Migrated to books&pages concept

### 4.1.0

+ Migrated to `th2-codec:4.8.0`
+ Common updated to `3.44.1`
+ Bom updated to `4.2.0`
+ Codec to `4.8.1`

### 4.0.1

+ Migrated to `th2-codec:4.7.6`

### 4.0.0

+ Migration to **books/pages** cradle `4.0.0`
+ common updated from `3.13.1` to `4.0.0`
+ bom updated from `2.10.1` to `3.1.0`
+ bintray switched to **sonatype**

+ Migrated to `th2-codec` core part. Uses the standard configuration format and pins for th2-codec
+ Updated `bom`: `3.1.0` -> `4.2.0`
+ Updated `common`: `3.37.1` -> `3.44.0`

### 3.2.1

+ fixed: last array-field as simple value

### 3.2.0

+ new length validation parameter
+ array support, to specify array need to have empty header for each value [^array]

### 3.1.0

+ reads dictionaries from the /var/th2/config/dictionary folder.
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd,
  default configuration
+ update Cradle version. Introduce async API for storing events
+ removed gRPC event loop handling
+ fixed dictionary reading
