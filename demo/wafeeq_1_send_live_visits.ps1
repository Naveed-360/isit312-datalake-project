# Wafeeq, live step 1: send 100 new clinic visits to Kafka (about 20 seconds)
Set-Location (Split-Path $PSScriptRoot -Parent)
python jobs\producer.py
