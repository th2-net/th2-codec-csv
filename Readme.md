# Csv codec
## Description
Designed for decode csv raw messages from csv reader to parsed messages
## Settings
Simulator using environment variables for settings
#### RABBITMQ_HOST
RabbitMQ host \
(Example: localhost)
#### RABBITMQ_PORT
RabbitMQ port \
(Example: 8080)
#### RABBITMQ_VHOST
RabbitMQ virtual host \
(Example: vh)
#### RABBITMQ_USER
RabbitMQ user \
(Example: guest)
#### RABBITMQ_PASS
RabbitMQ password \
(Example: guest)
#### RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY
RabbitMQ exchange name
#### TH2_CSV_CODEC_IN_QUEUE
RabbitMQ queue for listening csv raw messages from csv reader
#### TH2_CSV_CODEC_OUT_QUEUE
RabbitMQ queue for sending parsed messages