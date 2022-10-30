FROM gcr.io/tiroapp-4cb17/giorgio-worker
# Python dependencies
RUN /usr/local/bin/pip install html2text tqdm requests
COPY . /app/
WORKDIR /app
# Google Cloud credentials and the subscription containing our tasks
ENV GOOGLE_APPLICATION_CREDENTIALS /app/service_account_key.json
ENV SUBSCRIPTION "projects/tiroapp-4cb17/subscriptions/workers"
# Setup our output directory where the JSON files will be writen
ENV OUTPUT_PATH "/data"
VOLUME [ "/data" ]
# The is the Python module path to the module containing or tasks.
CMD ["count_words"]