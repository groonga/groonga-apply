FROM ruby:3.0-bullseye

RUN \
  echo "debconf debconf/frontend select Noninteractive" | \
    debconf-set-selections

COPY . groonga-sync
WORKDIR groonga-sync
RUN rake install
WORKDIR /
RUN rm -rf groonga-sync

ENTRYPOINT ["groonga-sync"]
CMD ["--server", "--dir=/var/lib/groonga/sync"]
