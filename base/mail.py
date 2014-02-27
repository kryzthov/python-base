#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

import email.mime.text
import getpass
import logging
import os
import smtplib
import socket

from base import base


FLAGS = base.FLAGS
LogLevel = base.LogLevel
Default = base.Default


FLAGS.AddString(
    name='smtp_server',
    default='localhost:25',
    help=('Default SMTP server to use to send emails.\n'
          'Empty means do not send emails.\n'
          'For example, GMail uses smtp.gmail.com:587 with TLS.'),
)

FLAGS.AddString(
    name='email_recipients',
    default=None,
    help=('Comma-separated list of default recipients for email notifications.\n'
          'None or empty means email notifications are diabled by default.'),
)

FLAGS.AddString(
    name='email_sender',
    default='%s@%s' % (getpass.getuser(), socket.getfqdn()),
    help=('Default sender email address for email notifications.\n'
          'None or empty means email notifications are disabled by default.'),
)

FLAGS.AddBoolean(
    name='smtp_use_tls',
    default=False,
    help=('Whether to use TLS when sending emails.'),
)

FLAGS.AddBoolean(
    name='smtp_auth',
    default=False,
    help=('Whether to perform SMTP authentication.'),
)

FLAGS.AddString(
    name='smtp_login',
    default=None,
    help=('Optional explicit login to use when authenticating.\n'
          'None or empty means use the sender address.'),
)

FLAGS.AddString(
    name='smtp_password_env',
    default='SMTP_PASSWORD',
    help=('Name of the environment variable that contains the password.'),
)


# ------------------------------------------------------------------------------


class Error(Exception):
  """Errors raised in this module."""
  pass


def SendMail(
    subject,
    body,
    sender=Default,
    recipients=Default,
    smtp_server=Default,
    use_tls=Default,
    login=Default,
    password=Default,
):
  """Sends an email.

  Args:
    subject: Email subject header (string).
    body: Email body content (string).
    sender: Optional explicit sender address.
        Default uses the global flag --email-sender.
    recipients: Optional explicit collection of recipient addresses.
        Default uses the global flag --email-recipients.
    smtp_server: Optional explicit SMTP server host:port to send with.
        None or empty string means do not actually send an email.
        Default uses the SMTP value from the global flag --smtp-server.
    use_tls: Whether to use TLS.
    login: SMTP login for authentication.
        Default is to use --email-login
    password: Password, when using TLS.
  """
  if smtp_server is Default:
    smtp_server = FLAGS.smtp_server
  if (smtp_server is None) or (len(smtp_server) == 0):
    logging.debug('No default SMTP server configured')
    return
  (host, port) = smtp_server.split(':')
  port = int(port)

  if sender is Default:
    sender = FLAGS.email_sender
  if (sender is None) or (len(sender) == 0):
    logging.debug('No default SMTP sender configured')
    return

  if recipients is Default:
    recipients = set()
    if FLAGS.email_recipients is not None:
      recipients.update(FLAGS.email_recipients.split(','))
  if len(recipients) == 0:
    logging.debug('No default SMTP recipients configured')
    return

  msg = email.mime.text.MIMEText(body)
  msg['Subject'] = subject
  msg['From'] = sender
  msg['To'] = ','.join(recipients)
  body = msg.as_string()

  logging.debug(
      'Sending email to %r with subject: %r and body:\n%s\n%s\n%s',
      recipients, subject, '-' * 80, body, '-' * 80)

  # Connect to SMTP server:
  server = smtplib.SMTP(host=host, port=port)
  reply = server.ehlo()
  logging.debug('SMTP handshake: %s:%d response is %r', host, port, reply)

  # Enable TLS if required:
  if use_tls is Default:
    use_tls = FLAGS.smtp_use_tls
  if use_tls:
    server.starttls()

  # Proceed with authentication, if requested:
  if login is Default:
    if FLAGS.smtp_auth:
      login = FLAGS.smtp_login
      if (login is None) or (len(login) == 0):
        login = sender
    else:
      login = None
  if (login is not None) and (len(login) > 0):
    if password is Default:
      password = os.environ[FLAGS.smtp_password_env]
    server.login(login, password)

  # Send email:
  try:
    server.sendmail(
        from_addr=sender,
        to_addrs=recipients,
        msg=body,
    )
  finally:
    server.quit()


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Error('Not a standalone program!')
