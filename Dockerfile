FROM python:3.10-bullseye
COPY . .

RUN apt update && apt install -y make
RUN make venv
RUN make install-deps

CMD ["/venv/bin/python", "main.py"]