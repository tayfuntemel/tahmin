name: "Quant Gunluk Kupon Pipeline"

on:
  schedule:
    - cron: "30 6 * * *" # Her gün TSİ 09:30'da otomatik çalışır
  workflow_dispatch:     # Manuel "Run workflow" butonu

permissions:
  contents: write

jobs:
  kupon_olustur:
    runs-on: ubuntu-latest

    env:
      DB_HOST: "netscout.fun"
      DB_USER: "netscout_veri"
      DB_PASSWORD: "i.34temel1"
      DB_NAME: "netscout_veri"
      DB_PORT: "3306"

    steps:
      - name: Repoyu Klonla
        uses: actions/checkout@v4

      - name: Python 3.11 Kur
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Kütüphaneleri Yükle
        run: |
          python -m pip install --upgrade pip
          pip install requests pymysql pandas numpy scipy scikit-learn lightgbm pyyaml

      - name: 1. Adım - Maçları ve Tahminleri Sonuçlandır (guncelle.py)
        run: python guncelle.py

      - name: 2. Adım - Gelecek Fikstürü Çek (cek_2.py)
        run: python cek_2.py

      - name: 3. Adım - Quant Model, Kalibrasyon ve Günlük Kupon (gunluk_calistir.py)
        run: python gunluk_calistir.py

      - name: Üretilen marketler.json Dosyasını Repoya Kaydet
        run: |
          if [ -f marketler.json ]; then
            git config --global user.name 'github-actions[bot]'
            git config --global user.email 'github-actions[bot]@users.noreply.github.com'
            git add marketler.json
            git commit -m "chore: marketler.json otomatik guncellendi" || echo "Degisiklik yok"
            git push
          fi
