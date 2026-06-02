#!/bin/bash

echo "===================================="
echo " Iniciando Arquitectura Distribuida "
echo "===================================="

mkdir -p data/mysql
mkdir -p data/mariadb
mkdir -p data/postgres

sudo docker compose up -d --build

echo ""
echo "Servicios levantados:"
sudo docker compose ps
