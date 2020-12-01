# Csv codec
## Description
Designed for decode csv raw messages from csv reader to the parsed messages.

## Expected batch format

The codec expects that the first message in the batch to decode will be a CSV header.
That message must have the following property in its metadata:
```properties
message.type=header
```
If batch doesn't have that message the default header from the configuration will be used.
If no default header is set the batch will be skipped, and an error event will be sent.

Each raw message must contain exactly one row with data.
If it has more than one row or doesn't have data at all an error event will be sent.

## Settings
Csv codec has following parameters:

```yaml
default-header: [A, B, C]
delimiter: ','
encoding: UTF-8
display-name: CodecCsv
```
**default-header** - the default header for this codec. It will be used if no header found in the received batch.

**delimiter** - the delimiter to split values in received data. The default value is `,`.

**encoding** - the encoding for the received data. The default value is `UTF-8`.

**display-name** - the name to set in the root event sent to the event store. All errors during decoding will be attached to that root event.
The default value for the name is `CodecCsv`.

## Pins

The CSV codec requires at leas one pin with the following attributes:
1. `raw` and `subscribe` - to receive raw data.
2. `parsed` and `publish` - to send decoded data.

The number of pins with each set of attributes is not limited. **Pins can use filters**.

## Full configuration example

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
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
      attributes: ['raw', 'subscribe']
    - name: out_codec_decode
      connection-type: mq
      attributes: ['parsed', 'publish']
```
