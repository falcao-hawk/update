#!/bin/bash
# Adiciona o crontab ao cron
crontab /etc/cron.d/meu-cronjob

# Inicia o serviço cron no foreground
cron -f
