import smtplib
from email.message import EmailMessage
import os

def send_email_with_attachment(
    to_email: str,
    subject: str,
    body: str,
    attachment_path: str,
    attachment_filename: str = None
):
    msg = EmailMessage()
    msg["From"] = os.environ.get("SMTP_FROM")
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with open(attachment_path, "rb") as f:
        data = f.read()
        filename = attachment_filename or attachment_path.split("/")[-1]

    msg.add_attachment(
        data,
        maintype="application",
        subtype="octet-stream",
        filename=filename
    )

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USERNAME")
    smtp_pass = os.environ.get("SMTP_PASSWORD")
    use_tls = os.environ.get("SMTP_USE_TLS", "true").lower() == "true"

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        if use_tls:
            server.starttls()
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)
        server.send_message(msg)