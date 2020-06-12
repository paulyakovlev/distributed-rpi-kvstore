FROM python:3.8-slim

RUN pip install Flask
RUN pip install requests
RUN pip install flask_script

WORKDIR /server
ADD . /server

CMD PYTHONHASHSEED=123 python server.py run_server

EXPOSE 8085