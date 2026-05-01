FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY vortex/ ./vortex/

RUN pip install --no-cache-dir -e .[admin,monitoring,serialization]

EXPOSE 9090 8090 9100

CMD ["python", "-c", "import asyncio; from vortex.broker import Broker; b=Broker(); asyncio.run(b.start()); print('vortex-mq node started'); asyncio.get_event_loop().run_forever()"]
