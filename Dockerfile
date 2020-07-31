FROM python:3.7

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir yarl==1.4.2

COPY ./ .

CMD [ "python", "run.py" ]
