## app/alerts.py

from __future__ import annotations
import os, smtplib, requests
from email.mime.text import MIMEText


def send_email(subject: str, body: str):
    host = os.getenv("SMTP_HOST"); user = os.getenv("SMTP_USER"); pwd = os.getenv("SMTP_PASS")
    to_addr = os.getenv("ALERT_EMAIL_TO")
    if not all([host, user, pwd, to_addr]):
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addr
    with smtplib.SMTP(host) as s:
        s.starttls(); s.login(user, pwd); s.sendmail(user, [to_addr], msg.as_string())


def send_slack(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url: return
    requests.post(url, json={"text": text}, timeout=5)
