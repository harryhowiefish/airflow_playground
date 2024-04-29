```
docker build -t custom_airflow .
docker compose up airflow-init
docker compose up -d
```


### Setup SMTP
[Google app password setup](https://support.google.com/accounts/answer/185833?hl=en)
```
smtp_host = smtp.gmail.com
smtp_user = <your email>
smtp_password = <your gmail app password>
smtp_port = 587
smtp_mail_from = <your email>
```


### Connect to GCP
- Setup GCP Service Account credentials ()
- Add credential to image (uncomment the two lines in Dockerfile)
- Rebuild image ```docker build --no-cache -t custom_airflow . ```
- Launch Airflow
- Run 