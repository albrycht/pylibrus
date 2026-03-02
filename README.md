## pyLibrus

Message scraper from crappy Librus Synergia gradebook. Forwards every new
message from a given folder to an e-mail.

## Linux installation (semi manual)

* Make sure you have installed `git`, `virtualenv` and `python3`
* Checkout **pylibrus** repository and run `install.sh` script
* Setup parameters in `check_librus.sh` (path provided in `install.sh` output)
* Setup cron entry to run script periodically (details in `install.sh` output)

## Manual usage

Parameters are passed through environment:
* `DB_NAME` - file with SQLite database of sent messages
* `LIBRUS_USER` - login to Librus
* `LIBRUS_PASS` - password to Librus
* `SMTP_USER` - login to `SMTP_SERVER` (also the originator of the e-mail sent)
* `SMTP_PASS` - password to `SMTP_SERVER`
* `SMTP_SERVER` - SMTP server address (e.g. `smtp.gmail.com`)
* `EMAIL_DEST` - destination to send e-mail, may be many e-mails separated by `,`

Example shell script to run in loop, to be launched from `tmux` or `screen`:

```bash
#!/bin/bash

source venv/bin/activate

set -xeuo pipefail

export LIBRUS_USER=...
export LIBRUS_PASS=...

export SMTP_USER=...
export SMTP_PASS=...
export SMTP_SERVER=...

export EMAIL_DEST=...

while true; do
        python pylibrus.py
        sleep 600
done
```

**WARNING**: only GMail SMTP server was tested.

## Webhook attachments in S3

Webhook notifications can send attachment links from Librus or from S3.

Configuration (in each webhook user section, e.g. `[user:ChildName2]`):
- `webhook_attachments_source=librus_link` keeps sends links to attachments on Librus webpage
- `webhook_attachments_source=s3://<bucket>/<optional-prefix>` uploads attachments to S3 and sends pre-signed links
- when using `s3://...` for that user, set:
  - `s3_region`
  - `s3_access_key_id`
  - `s3_secret_access_key`
  - optional `s3_session_token`
  - optional `s3_endpoint_url` (for S3-compatible storage)

S3 link expiration is fixed in code:
- `LINK_EXPIRE_DURATION = 604800` (7 days, max for S3 pre-signed URLs)

Required IAM permissions for provided credentials:
- `s3:PutObject`
- `s3:GetObject`

Objects are never deleted from bucket so please set correct lifecycle rules for your bucket. 
Recommend setting is to automatically remove files after 7 days. 

One bucket can be used for many users at the same time. 

## Potential improvements

* support HTML messages
* support announcements
* support calendar
