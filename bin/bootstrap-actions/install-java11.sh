#!/bin/bash
set -e
sudo yum install -y java-11-amazon-corretto
sudo update-alternatives --set java /usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java