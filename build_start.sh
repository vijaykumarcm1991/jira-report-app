#!/usr/bin/bash

#docker build --no-cache -t jira-report-app .

docker build -t jira-report-app .

docker run -d -p 8000:8000 \
  --env-file .env \
  jira-report-app
