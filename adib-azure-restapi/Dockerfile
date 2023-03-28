FROM python:3.8-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

COPY FlaskAppRest/ .

# CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
# RUN echo "export FLASK_RUN_PORT=2001" >> /etc/bash.bashrc

CMD [ "flask", "run","-h","0.0.0.0","-p","2001"]
