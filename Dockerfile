FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-bash-pipeline:latest-python3.8

# Setup scheduler-specific dependencies
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Setup local package
COPY . /opt/project
RUN pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run.sh"]

