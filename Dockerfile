FROM ubuntu/kafka:latest

RUN apt update && apt install -y vim

ENTRYPOINT ["entrypoint.sh"]
CMD ["/etc/kafka/server.properties"]
