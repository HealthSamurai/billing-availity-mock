# Stdlib-only Availity Coverage API mock — no third-party deps, so a slim base
# is all we need and the image stays tiny.
FROM python:3.13-slim

# Flush print() immediately so startup/stop lines show up in `kubectl logs`.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY availity_mock.py .

# Run as a non-root uid (readOnlyRootFilesystem-friendly: the mock writes nothing).
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin mock
USER 10001

EXPOSE 8090

# Behaviour is tuned via AVAILITY_MOCK_* env at deploy time (see README.md).
CMD ["python", "availity_mock.py"]
