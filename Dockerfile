# Expects CI-staged binaries (rmr-amd64, rmr-arm64) in build context.
# See .github/workflows/release.yml — `docker build .` from a clean checkout will fail.
FROM scratch
ARG TARGETARCH
COPY rmr-${TARGETARCH} /rmr
ENTRYPOINT ["/rmr"]
