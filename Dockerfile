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

# The image's Python 3.12 satisfies pyproject.toml, so uv needs no managed Python and the
# build needs nothing but PyPI (no apt, no GitHub downloads).
FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir uv==0.11.32
WORKDIR /app
COPY pyproject.toml uv.lock ./
# yt-dlp and deno (both from PyPI wheels) into .venv/bin, where config/youtube.yaml expects them.
ENV UV_PYTHON=/usr/local/bin/python3.12 UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy
RUN uv sync --frozen --no-install-project && uv cache clean
COPY config config
COPY --from=build /src/target/release/youtube-crawl /usr/local/bin/youtube-crawl
# Temp files from yt-dlp go here; give the container a local disk, not a network one.
ENV TMPDIR=/tmp
ENTRYPOINT ["youtube-crawl"]
CMD ["fetch", "--queue", "--jobs", "4"]
