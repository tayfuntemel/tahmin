name: 2- Yapay Zeka Tahmin Motoru

on:
  schedule:
    - cron: '*/30 * * * *' # Her 30 dakikada bir çalışır
  workflow_dispatch: 

jobs:
  run-tahmin:
    runs-on: ubuntu-latest
    steps:
      - name: Kodlari Cek
        uses: actions/checkout@v4
      
      - name: Python Kurulumu
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
          
      - name: Gerekli Kutuphaneleri Yukle
        run: |
          python -m pip install --upgrade pip
          pip install requests pymysql
          
      - name: Yapay Zeka Botunu Calistir
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
          DB_NAME: ${{ secrets.DB_NAME }}
          DB_PORT: ${{ secrets.DB_PORT }}
        run: python tahmin.py
