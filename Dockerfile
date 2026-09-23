# Orbit Watcher — private dashboard. Standard library only: no pip installs.
FROM python:3.12-slim
WORKDIR /app
COPY watcher.py server.py dashboard.html regime.py regime_skill.py ./
ENV PYTHONUNBUFFERED=1 DATA_DIR=/data
# Runs as root because Railway volumes are root-owned.
EXPOSE 8080
CMD ["python", "server.py"]
