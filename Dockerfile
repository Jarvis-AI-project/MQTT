FROM eclipse-mosquitto:latest

RUN apk add --no-cache python3 py3-pip
RUN pip3 install --no-cache-dir pymongo paho-mqtt

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/usr/sbin/mosquitto" ,"-c", "/mosquitto/config/mosquitto.conf"]
