FROM nginx:1.10

LABEL maintainer "j.vanderzwaan@esciencecenter.nl"

EXPOSE 80
COPY . /usr/share/nginx/html
