# A fetch worker: youtube-crawl plus the yt-dlp and deno that uv pins. Stateless; configure it with
# environment variables and mounts:
#   DATABASE_URL            the Postgres frontier (queue mode)
#   AWS_* / GOOGLE_*        store credentials, if the store is s3:// or gs://
#   /app/config/youtube.yaml  mount your own to override the defaults baked in
#   /run/secrets/proxies.txt  the proxy list (pass --proxies /run/secrets/proxies.txt)
# Example:
#   docker run --rm -e DATABASE_URL -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_ENDPOINT \
#     -v $PWD/proxies.txt:/run/secrets/proxies.txt:ro the-crawler \
#     --store s3://bucket/youtube fetch --queue --jobs 8 --proxies /run/secrets/proxies.txt

FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --break-system-packages uv==0.11.32
WORKDIR /app
COPY pyproject.toml uv.lock ./
# uv fetches a managed Python (the project wants >= 3.12) and installs yt-dlp and deno into .venv.
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_LINK_MODE=copy
RUN uv sync --frozen --no-install-project && uv cache clean
COPY config config
COPY --from=build /src/target/release/youtube-crawl /usr/local/bin/youtube-crawl
# Temp files from yt-dlp go here; give the container a local disk, not a network one.
ENV TMPDIR=/tmp
ENTRYPOINT ["youtube-crawl"]
CMD ["fetch", "--queue", "--jobs", "4"]
