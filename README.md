# 🏥 ICU Telemetry

Sistem pemrosesan dan pemantauan data telemetri pasien ICU (*Intensive Care Unit*) secara *real-time* menggunakan Apache Spark dan antarmuka web lokal.

---

## 📋 Prasyarat

Sebelum memulai, pastikan perangkat Anda telah memenuhi kebutuhan berikut:
* **Sistem Operasi:** Windows (menggunakan **Command Prompt / CMD**).
* **Python:** Sudah terinstal dan terdaftar di *System PATH* (dapat dicek dengan perintah `py --version`).
* **Direktori Project:** Buka CMD langsung di dalam folder utama project **ICU Telemetry**.

---

## 🚀 Panduan Instalasi

Ikuti langkah-langkah di bawah ini secara berurutan untuk menyiapkan *environment* lokal:

### 1. Buat Virtual Environment
Jalankan perintah berikut untuk mengisolasi seluruh *library* dan *dependencies* project:
```cmd
py -m venv .venv
```

### 2. Aktifkan Virtual Environment
Masuk ke dalam *virtual environment* yang baru saja dibuat:
```cmd
.venv\Scripts\activate
```
> 💡 **Verifikasi Aktivasi:**
> Pastikan muncul tanda **`(.venv)`** di sebelah kiri baris perintah CMD Anda (contoh: `(.venv) C:\...\ICU Telemetry>`). 
> * Jika belum muncul atau terminal terasa menggantung, tekan **Enter** (atau `Ctrl + C`), lalu jalankan perintah alternatif berikut:
> ```cmd
> .venv\Scripts\activate.bat
> ```

### 3. Instal Dependencies
Pastikan status `(.venv)` sudah aktif, kemudian instal seluruh paket yang dibutuhkan dari `requirements.txt`:
```cmd
pip install -r requirements.txt
```

---

## ▶️ Menjalankan Project

> ⚠️ **Penting:** Jika `spark_processor.py` berjalan secara berkelanjutan (*streaming*), biarkan **Terminal 1** tetap terbuka dan buka **Terminal 2** baru (dengan mengaktifkan `.venv` kembali) untuk menjalankan `app.py`.

1. **Jalankan Spark Processor (Pemroses Data)**
   ```cmd
   py spark_processor.py
   ```

2. **Jalankan Web Server (Dashboard)**
   ```cmd
   py app.py
   ```

3. **Buka Antarmuka Web**
   Setelah server aktif tanpa error, buka web browser favorit Anda dan akses URL berikut:
   👉 **[http://127.0.0.1:5000/](http://127.0.0.1:5000/)**

---

## 🛠️ Ringkasan Perintah Cepat (*Quick Start*)

Untuk penggunaan selanjutnya setelah instalasi awal selesai, cukup jalankan:
```cmd
.venv\Scripts\activate
py spark_processor.py
py app.py
```
