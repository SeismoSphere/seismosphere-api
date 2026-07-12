##### Lembar Kemajuan Skripsi
Nama Naufal Alif Zhafran FOTO
NIM 225150207111033
No HP 081332071836
Email naufalzhafran@student.ub.ac.i
d
Pembimbing 1 Dr. Ir. Diva Kurnianingtyas,
S.Kom.
Pembimbing 2 Dr. Ir. Arief Andy Soebroto, S.T.,
M.Kom.
Judul Analisis dan Klasifikasi Zona
Hazard Gempa di Asia Berbasis
Big Data Pipeline
Tanggal Mulai 6 Maret 2026
Tanggal pengumpulan Tanggal revisi Status ACC
7 April 2026
21 April 2026
28 April 2026
3 Mei 2026
12 Mei 2026
20 Mei 2026
10 Juni 2026
30 Juni 2026
15 April 2026
27 April 2026
3 Mei 2026
17 Mei 2026
25 Juni 2026
ACC
ANALISIS DAN KLASIFIKASI ZONA HAZARD GEMPA DI ASIA
BERBASIS BIG DATA PIPELINE
SKRIPSI
Untuk memenuhi sebagian persyaratan
memperoleh gelar Sarjana Komputer
Disusun Oleh:
Naufal Alif Zhafran
```
NIM : 225150207111033
```
PROGRAM STUDI TEKNIK INFORMATIKA
DEPARTEMEN TEKNIK INFORMATIKA
FAKULTAS ILMU KOMPUTER
UNIVERSITAS BRAWIJAYA
MALANG
2026
PENGAJUAN
ANALISIS DAN KLASIFIKASI ZONA HAZARD GEMPA DI ASIA BERBASIS BIG DATA
PIPELINE
SKRIPSI
Diajukan untuk memenuhi sebagian persyaratan
memperoleh gelar Sarjana Komputer
Disusun oleh:
Naufal Alif Zhafran
```
NIM:225150207111033
```
Telah diperiksa dan disetujui oleh:
Dosen Pembimbing I
Dr. Ir. Diva Kurnianingtyas, S.Kom.
```
NIK: 2022019612132001
```
Dosen Pembimbing 2
Dr. Ir. Arief Andy Soebroto, S.T., M.Kom.
```
NIP: 197204251999031002
```
Mengetahui
Ketua Departemen Teknik Informatika
Bayu Priyambadha, S.Kom., M.Kom., Ph.D.
```
NIP: 198209092008121004
```
i
PERNYATAAN ORISINALITAS
Saya menyatakan dengan sebenar-benarnya bahwa sepanjang pengetahuan
saya, di dalam naskah skripsi ini tidak terdapat karya ilmiah yang pernah diajukan
oleh orang lain untuk memperoleh gelar akademik di suatu perguruan tinggi,
dan tidak terdapat karya atau pendapat yang pernah ditulis atau diterbitkan oleh
orang lain, kecuali yang secara tertulis disitasi dalam naskah ini dan disebutkan
dalam daftar referensi.
Apabila ternyata di dalam naskah skripsi ini dapat dibuktikan terdapat
unsur-unsur plagiasi, saya bersedia skripsi ini digugurkan dan gelar akademik
```
yang telah saya peroleh (sarjana) dibatalkan, serta diproses sesuai dengan
```
```
peraturan perundang-undangan yang berlaku (UU No. 20 Tahun 2003, Pasal 25
```
```
ayat 2 dan Pasal 70).
```
Malang, 6 Maret 2026
Naufal Alif Zhafran
```
NIM: 225150207111033
```
ii
PRAKATA
Bagian ini memuat pernyataan resmi untuk menyampaikan rasa terima kasih
penulis kepada berbagai pihak yang telah membantu penyelesaian skripsi ini.
Nama-nama penerima ucapan terima kasih sebaiknya dituliskan lengkap,
termasuk gelar akademik, dan pihak-pihak yang tidak terkait dihindari untuk
dituliskan. Bahasa yang digunakan seharusnya mengikuti kaidah bahasa
Indonesia yang baku. Prakata boleh diakhiri dengan paragraf yang menyatakan
bahwa penulis menerima kritik dan saran untuk pengembangan penelitian
selanjutnya. Terakhir, prakata ditutup dengan mencantumkan kota dan tanggal
penulisan prakata, lalu diikuti dengan kata “Penulis”.
Malang, 6 Maret 2026
Penulis
email@domain.com
iii
ABSTRAK
Naufal Alif Zhafran, Analisis dan Klasifikasi Zona Hazard Gempa di Asia Berbasis
Big Data Pipeline
```
Pembimbing: Dr. Ir. Diva Kurnianingtyas, S.Kom. dan Dr. Ir. Arief Andy Soebroto,
```
S.T., M.Kom.
Wilayah Asia merupakan kawasan dengan tingkat aktivitas seismik yang
tinggi akibat interaksi berbagai lempeng tektonik utama. Kompleksitas
karakteristik gempa serta cakupan wilayah yang luas menyebabkan analisis dalam
lingkup satu negara menjadi kurang memadai untuk memahami pola seismik
secara komprehensif. Oleh karena itu, diperlukan pendekatan yang mampu
menganalisis distribusi gempa secara spasial serta mengklasifikasikan tingkat
risiko gempa dalam skala regional.
Penelitian ini bertujuan untuk menganalisis pola distribusi gempa di kawasan
Asia menggunakan metode clustering berbasis densitas, yaitu Density-Based
```
Spatial Clustering of Applications with Noise (DBSCAN), serta mengklasifikasikan
```
tingkat risiko gempa menggunakan algoritma machine learning, yaitu Random
Forest dan XGBoost. Data gempa yang digunakan merupakan data sekunder yang
diperoleh dari berbagai sumber terbuka. Dalam mendukung proses integrasi dan
pengolahan data dalam skala besar, digunakan pendekatan big data pipeline
sebagai metode pendukung.
Hasil dari penelitian ini menunjukkan bahwa metode DBSCAN mampu
mengidentifikasi pola distribusi gempa dalam bentuk cluster yang
merepresentasikan zona aktivitas seismik. Selanjutnya, hasil clustering digunakan
sebagai fitur dalam proses klasifikasi untuk menentukan tingkat risiko gempa.
Evaluasi model dilakukan menggunakan metrik accuracy, precision, recall, dan
F1-score untuk membandingkan performa algoritma yang digunakan.
Penelitian ini diharapkan dapat memberikan kontribusi dalam
pengembangan metode analisis spasial gempa berbasis data serta menjadi
referensi dalam upaya mitigasi bencana pada skala regional di kawasan Asia.
Kata kunci: gempa bumi, analisis geospasial, DBSCAN, klasifikasi, Random Forest,
XGBoost, big data pipeline
iv
ABSTRACT
Naufal Alif Zhafran, Analysis and Classification of Earthquake Hazard Zones in
Asia Based on Big Data Pipeline
```
Pembimbing: Dr. Ir. Diva Kurnianingtyas, S.Kom. dan Dr. Ir. Arief Andy Soebroto,
```
S.T., M.Kom.
Asia is one of the regions with the highest seismic activity in the world due to
the interaction of major tectonic plates. The complexity of earthquake
characteristics and the wide geographical coverage make single-country analysis
insufficient to comprehensively understand seismic patterns. Therefore, an
approach is needed to analyze the spatial distribution of earthquakes and classify
earthquake risk levels on a regional scale.
This study aims to analyze the spatial distribution patterns of earthquakes in
Asia using a density-based clustering method, namely Density-Based Spatial
```
Clustering of Applications with Noise (DBSCAN), and to classify earthquake risk
```
levels using machine learning algorithms, namely Random Forest and XGBoost.
The earthquake data used in this study are secondary data obtained from various
open sources. To support large-scale data integration and processing, a big data
pipeline approach is utilized as a supporting method.
The results of this study indicate that the DBSCAN method is capable of
identifying earthquake distribution patterns in the form of clusters representing
seismic activity zones. Furthermore, the clustering results are used as features in
the classification process to determine earthquake risk levels. Model evaluation is
conducted using accuracy, precision, recall, and F1-score to compare the
performance of the applied algorithms.
This study is expected to contribute to the development of data-driven spatial
analysis methods for earthquakes and serve as a reference for disaster mitigation
efforts at a regional scale in Asia.
```
Keywords: earthquake, geospatial analysis, DBSCAN, classification, Random Forest,
```
XGBoost, big data pipeline
v
DAFTAR ISI
PENGAJUAN i
PERNYATAAN ORISINALITAS ii
PRAKATA iii
ABSTRAK iv
ABSTRACT v
DAFTAR ISI vi
DAFTAR TABEL viii
DAFTAR GAMBAR ix
DAFTAR LAMPIRAN x
BAB 1 PENDAHULUAN 1
1.1 Latar Belakang 1
1.2 Rumusan Masalah 3
1.3 Tujuan Penelitian 3
1.4 Manfaat Penelitian 4
1.4.1 Manfaat Signifikansi Akademis dan Teoritis 4
1.4.2 Manfaat Signifikansi Praktis dan Aplikasi Kebijakan 5
1.5 Batasan Masalah 5
1.6 Sistematika Pembahasan 6
BAB 2 LANDASAN KEPUSTAKAAN 8
2.1 Kajian Pustaka 8
2.2 Analisis Geospasial dan Data Spasial 9
```
2.3 Clustering (HDBSCAN) 10
```
```
2.4 Klasifikasi (Random Forest and XGBoost) 11
```
2.4.1 Random Forest 12
```
2.4.2 Extreme Gradient Boosting (XGBoost) 12
```
2.5 Evaluasi Kinerja Model Klasifikasi 13
BAB 3 METODOLOGI PENELITIAN 15
3.1 Arsitektur System 15
3.2 Data Recollection dan Preprocessing 16
3.3 Metode Clustering Spasial 16
vi
3.4 Penentuan Fitur Analitik Spasial 17
3.5 Metode Klasifikasi Risiko 18
3.6 Evaluasi Model 19
BAB 4 PERANCANGAN & IMPLEMENTASI 20
4.1 Perancangan Sistem dan Arsitektur Big Data Pipeline 20
```
4.2 Perancangan Skema Pangkalan Data dan Fitur Keruangan (Geospatial Feature
```
```
Engineering) 20
```
4.3 Perancangan Model Analitik Geospasial dan Prediksi Risiko Bahaya 21
4.4 Implementasi Sistem Komputasi dan Validasi Hasil Model 22
4.4.1 Recollection Dan Preprocessing Data 22
4.4.2 Spatial Clustering HDBSCAN 25
```
4.4.3 Klasifikasi Risiko Zona Bahaya (Hazard Zone Classification) 26
```
4.4.3.1 Arsitektur Klasifikasi Berbasis Supervised Learning 27
4.4.3.2 Rekayasa Fitur Geofisika dan Spasial yang Ekstensif 27
4.4.3.3 Konstruksi Aliran Pemrosesan dan Persiapan Data 28
4.4.3.4 Struktur Komprehensif Rekayasa Perangkat Lunak
Terintegrasi 29
4.4.3.5 Penyatuan Fungsional dengan Orkestrasi Apache Airflow
DAG 29
BAB 5 HASIL & PEMBAHASAN 31
5.1 Kinerja Infrastruktur Big Data Pipeline 31
```
5.2 Pola Distribusi Spasial (HDBSCAN) 33
```
5.3 Evaluasi Kinerja Algoritma Klasifikasi 36
```
5.3.1 Signifikansi Inklusi Titik Anomali (Noise Inclusion) dalam
```
Formulasi Klasifikasi 36
```
5.3.2 Analisis Komparatif Dimensi Performa (Random Forest vs
```
```
XGBoost) 36
```
```
5.3.3 Dekonstruksi Matriks Kekeliruan (Confusion Matrix) 38
```
5.4 Implikasi Hasil Penelitian Terhadap Mitigasi Bencana 39
BAB 6 PENUTUP 40
DAFTAR REFERENSI 41
vii
DAFTAR TABEL
Tabel 2.1 Perbandingan Penelitian Terdahulu 8
Table 5.1 Hasil Spasial Preprocessing Dengan Magnitude Tertinggi 32
Table 5.2 Filtering Risk Label Berdasarkan Total Earthquake Recollection 35
Table 5.3 Rekapitulasi Komparatif Evaluasi Metrik Prediksi Arsitektur Algoritma 37
viii
DAFTAR GAMBAR
Gambar 2.1 Ilustrasi Ketangguhan HDBSCAN Terhadap Variasi Kepadatan Data
```
(Varying Densities) 10
```
Gambar 3.1 Diagram Alur Pelaksanaan Penelitian 15
Gambar 4.1 Log Eksekusi Raw Data Ingestion Pada Apache Airflow 23
Gambar 4.2 Log Eksekusi Proses Cleaning dan Reduction Data Pada Apache
Airflow 24
Gambar 4.3 Log Eksekusi Proses Enrichment Spasial dan Pemuatan Data Pada
Apache Airflow 25
Gambar 4.4 Log Eksekusi Proses Clustering HDBSCAN Pada Apache Airflow 26
Gambar 4.5 Logs Data Splitting 28
Gambar 4.6 Logs Training Classification Models 29
```
Gambar 5.1 Hasil Spasial Preprocessing Dalam PostGIS (Warehouse) 31
```
Gambar 5.2 Diagram Earthquake Distribution by Year 33
```
Gambar 5.3 Visualisasi Cluster Gempa HDBSCAN Dengan Titik Noise (Anomali) 34
```
```
Gambar 5.4 Visualisasi Cluster Gempa HDBSCAN Tanpa Titik Noise (Anomali) 35
```
Gambar 5.5 Logs Tahap Evaluasi 37
Gambar 5.6 Hasil Evaluasi Klasifikasi 37
Gambar 5.7 Heatmap Random Forest Confusion Matrix 38
Gambar 5.8 Heatmap XGBoost Confusion Matrix 39
ix
DAFTAR LAMPIRAN
LAMPIRAN A
x
BAB 1 PENDAHULUAN
1.1 Latar Belakang
Wilayah Asia merupakan salah satu kawasan geologis dengan tingkat
aktivitas seismik dan vulkanik tertinggi di dunia, sebuah kondisi yang secara
langsung dipengaruhi oleh dinamika interaksi tektonik antar berbagai lempeng
```
utama bumi (Hinga, 2015). Kawasan ini secara geografis didominasi oleh
```
kehadiran Sabuk Sirkum-Pasifik, yang lebih dikenal sebagai Cincin Api Pasifik
```
(Pacific Ring of Fire), yang menyumbang sekitar 90% dari seluruh kejadian gempa
```
```
bumi global (Morante-Carballo et al., 2024). Interaksi mekanis di batas-batas
```
lempeng ini tidak hanya menghasilkan pembentukan rangkaian busur kepulauan
vulkanik, tetapi juga melepaskan energi regangan tektonik yang terakumulasi
selama ratusan hingga ribuan tahun, memicu gempa bumi dengan tingkat
```
destruksi masif (Natawidjaja et al., 2021). Kondisi ini menyebabkan tingginya
```
frekuensi dan kompleksitas kejadian gempa bumi di berbagai wilayah Asia, di
mana dalam kurun waktu 50 tahun pengamatan khusus untuk sub-kawasan Asia
```
Tenggara saja telah tercatat total 13.426 kejadian gempa bumi signifikan (USGS,
```
```
2018). Oleh karena itu, pemahaman yang komprehensif, berbasis data, dan
```
berskala makro mengenai pola distribusi spasial serta probabilitas kejadian
```
seismik di kawasan ini menjadi sangat esensial bagi upaya mitigasi bencana (Ke et
```
```
al., 2025).
```
Untuk mengilustrasikan besarnya ancaman seismik di kawasan Asia secara
kuantitatif, analisis statistik frekuensi gempa bumi memperlihatkan tren yang
sangat mengkhawatirkan dan persisten dari tahun ke tahun. Berdasarkan data
historis, negara-negara kepulauan yang berada tepat di atas zona subduksi
menanggung beban tektonik yang luar biasa, Filipina misalnya mencatat 8.238
kejadian gempa dalam lima dekade, di mana 96 di antaranya adalah gempa bumi
```
destruktif berskala di atas 6.1 Magnitudo (USGS, 2018). Sementara itu, wilayah
```
lain seperti Taiwan dan Myanmar juga secara periodik mencatat ribuan tremor
```
seismik yang merusak infrastruktur vital (Harig et al., 2020). Selain itu, laporan
```
global mengindikasikan bahwa jumlah korban jiwa terus berfluktuasi pada tingkat
yang tinggi akibat gempa-gempa berskala menengah hingga besar di sepanjang
```
Cincin Api Asia (Morante-Carballo et al., 2024). Data distribusi frekuensi gempa
```
global ini memberikan landasan empiris yang kuat terhadap urgensi pemodelan
bahaya seismik.
Meskipun paparan bahaya seismik di Asia sangat tinggi dan didukung oleh
data kuantitatif yang solid, pendekatan konvensional dalam Penilaian Bahaya
```
Seismik (Seismic Hazard Assessment / SHA) saat ini masih memiliki kelemahan
```
metodologis yang fundamental. Kelemahan utama terletak pada kecenderungan
untuk membatasi analisis pemodelan secara ketat pada skala batas geopolitik
```
tingkat negara atau National Seismic Hazard Models (Gerstenberger et al., 2020).
```
Fenomena geologis dan propagasi gelombang seismik secara alamiah tidak
mematuhi batas yurisdiksi administratif antarnegara, sehingga memotong analisis
1
pada perbatasan artifisial sering kali menghasilkan estimasi parameter seismisitas
yang tidak stabil akibat kelangkaan sampel data pada zona patahan yang
```
terpotong (Sharma et al., 2024). Lebih lanjut, metode analisis spasial yang
```
digunakan secara nasional umumnya belum mampu mengidentifikasi pola
seismik secara akurat, mengingat peta konvensional sering kali bergantung pada
```
asumsi penyebaran seismisitas regional yang diseragamkan (uniform regional
```
```
seismicity) dan gagal memetakan heterogenitas patahan buta (Geller, 2011).
```
Oleh karena itu, diperlukan suatu pendekatan makro yang melampaui batas
negara karena peristiwa gempa besar terbukti memicu kerusakan struktural dan
tsunami yang dampaknya melintasi berbagai yurisdiksi, sehingga membutuhkan
```
strategi mitigasi regional yang terpadu (Morante-Carballo et al., 2024).
```
Menyikapi keterbatasan pemodelan skala nasional serta tuntutan untuk
menganalisis katalog gempa secara transnasional, pendekatan analisis yang
dibutuhkan tidak hanya harus bergeser dari nasional ke regional, tetapi juga
harus didukung oleh arsitektur pengelolaan data yang masif dan terorkestrasi.
Integrasi inisiatif multinasional menyoroti betapa pentingnya harmonisasi
```
informasi seismisitas yang melampaui jutaan titik data observasi (Pilz et al.,
```
```
2024). Dalam konteks ini, penggunaan pendekatan Big Data Pipeline tidak lagi
```
sekadar opsi teknis, melainkan sebuah prasyarat operasional absolut untuk
```
memproses metadata seismik dari Application Programming Interface (API)
```
```
secara kontinu (Taherkordi et al., 2020). Ekstraksi, transformasi, dan pemuatan
```
```
data (ETL) lintas negara dengan arsitektur pipeline data memastikan terjadinya
```
```
agregasi data multivariabel secara otomatis, pembersihan derau (noise), dan
```
transformasi geometri spasial berkecepatan tinggi sebelum data diumpankan ke
```
dalam basis data analitik geospasial relasional (Ke et al., 2025).
```
Di atas arsitektur basis data spasial yang telah diharmonisasi tersebut,
metode ekstraksi pengetahuan yang sangat teruji secara spasial diperlukan untuk
mengelompokkan episentrum gempa ke dalam zona-zona tektonik aktif. Dalam
penelitian ini, algoritma hierarkis Hierarchical Density-Based Spatial Clustering of
```
Applications with Noise (HDBSCAN) dipilih secara spesifik karena kemampuannya
```
mendeteksi klaster spasial dengan bentuk geometris yang tidak beraturan,
```
memanjang, dan saling tumpang-tindih mengikuti garis patahan bumi (Campello
```
```
et al., 2013). Penggunaan algoritma sentroid konvensional seperti K-Means tidak
```
relevan di sini karena K-Means mengasumsikan varians klaster yang selalu bulat
```
dan sangat rentan terhadap pencilan data (Wijaya et al., 2024). Sebaliknya,
```
HDBSCAN menyempurnakan batasan varians densitas tunggal dengan
membangun struktur hierarki konektivitas spasial yang mampu mengeksklusi
```
titik-titik data berdensitas rendah (noise) secara otonom (McInnes & Healy,
```
```
2017). Kemampuan untuk mengisolasi mikroseismisitas acak ini secara dramatis
```
meningkatkan rasio Signal-to-Noise pada dataset historis gempa, menjadikannya
```
metode yang paling rasional dan akurat untuk pemetaan seismik regional (Hafid
```
```
et al., 2024).
```
Meskipun identifikasi pola klasterisasi geospasial menggunakan HDBSCAN
sanggup mengekstraksi peta zona tektonik secara presisi, metodologi
2
tak-terawasi tersebut memiliki keterbatasan ontologis, yakni tidak dirancang
```
untuk memprediksi probabilitas tingkat bahaya (hazard risk severity) dari
```
gempa-gempa pada zona tersebut. Oleh karena itu, arsitektur Machine Learning
terawasi harus diimplementasikan dengan memanfaatkan label klaster HDBSCAN
```
sebagai fitur prediktif baru yang vital 1 (Panda & Yadav, 2025). Penggunaan
```
algoritma Random Forest dijustifikasi oleh kemampuannya yang sangat tangguh
dalam memproses volume data seismik besar secara paralel dan kemampuannya
mereduksi masalah overfitting yang sering terjadi pada data kebencanaan yang
```
asimetris 2 (Puspita et al., 2025). Di sisi lain, Extreme Gradient Boosting
```
```
(XGBoost) dipilih karena mengadopsi mekanisme boosting pohon berurutan yang
```
secara iteratif mengoreksi kesalahan komputasi menggunakan optimasi gradient
descent, sebuah teknik yang secara konsisten terbukti mengungguli model
```
konvensional pada kasus klasifikasi risiko (Saleem et al., 2025). Dengan
```
mengomparasikan kedua metodologi raksasa kelas ensambel ini, penelitian akan
mendelineasi keunggulan metrik operasional secara nyata untuk prediksi
```
probabilitas bahaya kegempaan (Mz et al., 2024).
```
1.2 Rumusan Masalah
Dinamika aktivitas seismik di kawasan Asia yang kompleks dan lintas batas
negara menimbulkan tantangan dalam analisis risiko gempa. Pendekatan berbasis
wilayah administratif sering kali tidak mampu merepresentasikan pola seismik
secara utuh karena sifat pergerakan lempeng yang tidak mengenal batas
geografis. Oleh karena itu, diperlukan pendekatan berbasis big data pipeline dan
machine learning untuk mengelola serta menganalisis data seismik dalam skala
besar dan heterogen. Dalam penelitian ini, dilakukan evaluasi terhadap
kemampuan DBSCAN dalam mengidentifikasi pola spasial gempa, serta pengujian
algoritma Random Forest dan XGBoost dalam mengklasifikasikan tingkat risiko
berdasarkan hasil clustering. Berdasarkan permasalahan tersebut, dirumuskan
beberapa pertanyaan penelitian sebagai berikut:
1. Bagaimana mengidentifikasi pola distribusi spasial gempa di kawasan Asia
```
menggunakan metode clustering berbasis densitas (HDBSCAN)?
```
2. Bagaimana hasil clustering dapat dimanfaatkan sebagai fitur dalam proses
klasifikasi tingkat risiko gempa?
3. Bagaimana algoritma Random Forest dan XGBoost dapat digunakan untuk
```
mengklasifikasikan tingkat risiko gempa (Low, Medium, High, dan Very High)
```
di kawasan Asia?
4. Bagaimana performa model klasifikasi dalam mengklasifikasikan tingkat
risiko gempa berdasarkan metrik evaluasi seperti accuracy, precision, recall,
dan F1-score?
1.3 Tujuan Penelitian
Mengacu secara koheren dan presisi pada rangkaian pernyataan perumusan
masalah yang telah diartikulasikan sebelumnya, riset ini didesain tidak hanya
untuk menyajikan narasi metodologis tetapi juga untuk meraih wawasan praktikal
3
dalam sains geospasial. Secara spesifik dan operasional, penelitian ini
mendefinisikan beberapa target pencapaian yang terukur dan berkorelasi
langsung dengan inovasi teknik klasifikasi geologis dan pembangunan
infrastruktur data. Uraian lengkap mengenai tujuan spesifik dari penelitian ini
dirangkum dalam penjabaran analitik berikut:
1. Menganalisis pola distribusi spasial gempa di kawasan Asia menggunakan
```
metode clustering berbasis densitas (HDBSCAN) untuk mengidentifikasi zona
```
aktivitas seismik.
2. Mengidentifikasi pola seismic corridor dan interaksi antar zona gempa lintas
negara berdasarkan hasil clustering yang diperoleh.
3. Mengklasifikasikan tingkat risiko gempa di kawasan Asia menggunakan
algoritma machine learning, yaitu Random Forest dan XGBoost, ke dalam
kategori risiko tertentu.
4. Mengevaluasi performa model klasifikasi dalam mengklasifikasikan tingkat
risiko gempa menggunakan metrik evaluasi seperti accuracy, precision,
recall, dan F1-score.
5. Memanfaatkan big data pipeline sebagai pendukung dalam proses integrasi
dan pengolahan data gempa lintas negara secara efisien.
1.4 Manfaat Penelitian
Keberhasilan dalam menjawab rumusan masalah secara saintifik melalui
tahapan pencapaian tujuan riset yang dirancang diharapkan membuahkan
spektrum implikasi positif yang substansial. Dampak kontributif ini berpotensi
memengaruhi lanskap akademis dalam kajian algoritma pemrosesan spasial dan
pada saat yang bersamaan menyajikan solusi teknokratis terhadap masalah tata
kelola risiko bencana di belahan bumi Asia yang rapuh. Manfaat komprehensif
dari penelitian ini diklasifikasikan ke dalam ranah epistemologis keilmuan
akademis serta ranah pragmatis aplikasi lapangan.
1.4.1 Manfaat Signifikansi Akademis dan Teoritis
Penelitian ini memproyeksikan kontribusi signifikan terhadap pendalaman
literatur ilmu komputer lintas disiplin, khususnya pada irisan kajian Machine
```
Learning dan Analisis Geospasial (Geo-informatics). Laporan ini
```
mendokumentasikan sintesis nyata mengenai kompatibilitas dan batas performa
```
penggabungan klaster densitas spasial (HDBSCAN) dengan kerangka
```
```
pengklasifikasi ensemble trees bertingkat tinggi (XGBoost dan Random Forest)
```
pada tipe data multivariat bervolume masif. Lebih jauh lagi, wacana keilmuan
akan diperkaya oleh bukti objektif bahwa analisis seismik dengan batas observasi
regional secara matematis mampu mereduksi ketidakpastian epistemik
```
(epistemic uncertainty) yang kronis dan inheren dalam kerangka National Seismic
```
```
Hazard Models (NSHMs) berbasis lokal. Secara rekayasa perangkat lunak, studi ini
```
meretas jalan operasional dalam mengembangkan cetak biru Big Data Pipeline
arsitektur pengolahan spasial modern untuk sains data yang mengeksploitasi
orkestrasi terotomatisasi secara efisien.
4
1.4.2 Manfaat Signifikansi Praktis dan Aplikasi Kebijakan
Pada tataran pelaksanaan publik, wawasan yang digali dari penelitian ini
merupakan aset strategis bagi para pembuat kebijakan regional, otoritas
kebencanaan lintas yurisdiksi, maupun entitas organisasi perserikatan
bangsa-bangsa di lingkup Asia-Pasifik. Peta visualisasi hazard seismik berbasis
klasifikasi prediktif data historis dapat digunakan sebagai katalis landasan
penyusunan kebijakan mitigasi multi negara mentransformasikan tata kelola
```
masalah kebencanaan yang selama ini dianggap "pelik" (wicked problem) akibat
```
asimetri informasi dan kekurangan visibilitas jangka panjang menjadi persoalan
```
yang lebih terukur dan dapat dikelola secara proaktif (tame problem). Kapabilitas
```
algoritma yang divalidasi memberikan parameter teknis rasional bagi alokasi
```
pembiayaan infrastruktur tahan gempa (seismic retrofitting policies), di mana
```
```
intervensi dapat difokuskan pada klaster geografis padat (hotspots) yang
```
berpotensi tinggi memicu kegagalan struktur katastropik. Arsitektur pipeline
otomatisasi data juga secara langsung memfasilitasi institusi geofisika nasional
untuk menyerap integrasi data lintas wilayah tanpa penambahan beban
komputasi manual.
1.5 Batasan Masalah
Dalam upaya menegakkan validitas akademik yang kokoh, mempertahankan
fokus objek analitik yang linier dengan paradigma kajian data science, dan
mengeliminir ambiguitas operasional akibat disparitas metode yang berada di
luar kapasitas sumber daya perangkat komputasional, penelitian ini diikat oleh
beberapa ketentuan batasan masalah yang absolut. Pembatasan variabel spesifik
ini difokuskan pada rekayasa delineasi masalah konseptual geomorfologi maupun
limitasi instrumen perumusan probabilitas algoritma:
1. Batasan Wilayah Pemetaan Regional: Penyelidikan spasial dan klasterisasi
geografi ditarik pada parameter bounding box koordinat benua Asia secara
kolektif dan eksklusif. Area ini merangkul secara ekstensif patahan
perbatasan cincin api dan zona interaksi tektonik Lempeng Eurasia, Lempeng
Indo-Australia, dan tepi Lempeng Pasifik, dengan mengabaikan anomali
deformasi kegempaan yang terisolasi sepenuhnya di benua Afrika atau
lempeng di luar kawasan cakupan.
2. Sifat Katalog dan Variabel Basis Data: Objek data yang dikomputasi
diklasifikasikan sebagai data sekunder pasif historis yang diperoleh dengan
memanggil API arsip lembaga berwenang, United States Geological Survey
```
(USGS). Penelitian membatasi diri pada metadata statistik seismisitas (titik
```
episentrum lintang dan bujur, hiposentrum kedalaman, besaran indeks
```
magnitudo, serta runtutan skala waktu kronologis kejadian), bukan analisis
```
```
spektrum spektral gelombang primer (P-wave) dan sekunder (S-wave)
```
```
beresolusi tinggi (seismogram fisis).
```
3. Rentang Varians Magnitudo Terukur: Untuk mengendalikan kebisingan
komputasional terhadap kepadatan spasial yang terlalu rapat dan
```
meminimalisasi pembentukan klaster palsu (pseudo-clusters) dari aktivitas
```
5
tektonik mikro tak berdampak, ekstraksi basis data menerapkan filter
```
ambang batas spesifik (misalnya mengisolasi peristiwa gempa bumi hanya
```
pada magnitudo ke atas, sejalan dengan standar gempa merusak𝑀𝑤 ≥ 4. 0
```
struktural).
```
4. Eksklusivitas Arsitektur Pemodelan Komputasional: Spektrum algoritma
yang disimulasikan, dioptimalkan hiper parameternya, dan diuji akurasinya
dibatasi secara tegas. Pengelompokan tak-terawasi mutlak menggunakan
varian DBSCAN sebagai tolok ukur analitik spasial kepadatan noise,
sementara agregasi Machine Learning terawasi bersandar sepenuhnya pada
Random Forest dan XGBoost, tanpa membandingkannya dengan kerangka
```
arsitektur pembelajaran mendalam spasio-temporal komposit (seperti
```
```
Convolutional Neural Networks berbasis keruangan) yang memerlukan
```
topologi komputasi silang yang sangat berbeda.
5. Skop Fungsionalitas Mitigasi: Konstruksi penelitian ini didesain sebagai studi
perumusan taksonomi probabilitas analitik statistik dan klasifikasi visual
pasca-kejadian agregat, bukan merupakan pengembangan perangkat lunak
rekayasa deterministik fisik patahan batuan dan sama sekali tidak
dikembangkan menjadi sistem alarm peringatan dini kegempaan terdistribusi
```
secara seketika (Earthquake Early Warning System / EEWS) interaktif.
```
1.6 Sistematika Pembahasan
Guna mendistribusikan alur penalaran akademis secara sistematis,
meningkatkan keterbacaan argumentasi nyata bagi komunitas cendekiawan, dan
memenuhi pedoman penulisan karya ilmiah universitas yang terstandardisasi,
struktur pengorganisasian laporan penelitian ini dirangkai melalui pembagian ke
dalam beberapa bab krusial. Setiap bab memikul fondasi elaboratif yang
mendukung kontinuitas bab sesudahnya, dengan sistematika deskriptif sebagai
```
berikut:
```
1. Bab I Pendahuluan
Bab ini berisi latar belakang penelitian, rumusan masalah, tujuan penelitian,
manfaat penelitian, batasan masalah, serta sistematika penulisan.
2. Bab II Landasan Kepustakaan
Bab ini memuat kajian teori dan penelitian terdahulu yang relevan dengan
topik penelitian, meliputi konsep gempa bumi, analisis geospasial, metode
```
clustering (HDBSCAN), serta algoritma machine learning yang digunakan
```
dalam klasifikasi.
3. Bab III Metodologi Penelitian
Bab ini menjelaskan tahapan penelitian yang dilakukan, mulai dari
pengumpulan data, preprocessing, penerapan metode clustering dan
klasifikasi, hingga evaluasi model yang digunakan dalam penelitian.
4. Bab IV Perancangan & Implementasi
Bab ini menguraikan cetak biru rancang bangun arsitektur sistem serta
dokumentasi langkah implementasi yang meliputi data ingestion, clustering
spasial, klasifikasi tingkat risiko zona bahaya, hingga pembentukan visualisasi
frontend interaktif map Pacific Ring of Fire.
6
5. Bab V Pembahasan
Bab ini menyajikan hasil dari penerapan metode, analisis kinerja infrastruktur
pipeline, algoritma klasifikasi dan visualisasi interaktif, serta implikasinya
terhadap mitigasi bencana.
6. Bab VI Penutup
Bab ini berisi kesimpulan dari hasil penelitian serta saran untuk
pengembangan penelitian selanjutnya.
7
BAB 2 LANDASAN KEPUSTAKAAN
2.1 Kajian Pustaka
Penelitian mengenai aktivitas seismik dan prediksi hazard telah banyak
dilakukan dengan mengadopsi berbagai pendekatan algoritma pembelajaran
mesin dan analisis geospasial secara bertahap. Perkembangan riset ini
menandakan adanya pergeseran fokus dari penilaian bahaya teritorial menjadi
penilaian regional yang mempertimbangkan pola kepadatan spasial gempa
```
secara holistik. Wijaya et al. (2024) mengimplementasikan metode analitik
```
algoritma DBSCAN untuk menganalisis data gempa bumi di Sulawesi pada
periode 2019-2023. Melalui penelitiannya, terbukti bahwa DBSCAN tidak hanya
efektif dalam mengidentifikasi pola kepadatan spasial episentrum pada patahan,
```
tetapi juga mampu mendeteksi dan mengeksklusi anomali (noise) yang tidak
```
relevan dari sinyal seismik berdensitas rendah.
Pada skala regional yang lebih makro, tinjauan literatur sistematis oleh
```
Morante-Carballo et al. (2024) menyoroti karakteristik kerentanan struktural
```
```
kawasan pesisir di zona Cincin Api Pasifik (Pacific Ring of Fire). Kajian ini secara
```
nyata menjustifikasi urgensi pengembangan kerangka mitigasi kebencanaan lintas
```
negara (transnasional) akibat karakteristik patahan tektonik yang saling
```
terhubung di kawasan Asia. Lebih jauh ke dalam ranah prediksi risiko
menggunakan pembelajaran mesin, algoritma berkelas ensemble menunjukkan
supremasinya dalam mereduksi margin galat prediksi spasial. Studi pada tahun
2024 yang mengadopsi model klasifikasi Random Forest untuk mengestimasi
taksonomi kejadian gempa bumi di Indonesia telah mencapai akurasi
keseluruhan sebesar 90,78% dengan memproses variabel-variabel historis dan
spasial. Di sisi lain, integrasi rekayasa pembelajaran hibrida bernama
```
Neural-XGBoost (N-XGB) mampu mencapai tingkat akurasi 94,8% dan F1-score
```
rata-rata sebesar 0,96 khusus untuk pemetaan kelas kebencanaan gempa bumi
```
multivariabel yang tidak seimbang (imbalanced dataset). Untuk mengekstraksi
```
research gap atau celah pembaruan dalam penelitian ini, Tabel 2.1 menyajikan
matriks perbandingan metodologi komprehensif antara penelitian terdahulu dan
usulan dalam rancang bangun skripsi ini.
Tabel 2.1 Perbandingan Penelitian Terdahulu
```
Penulis (Tahun) Topik Penelitian Metode/Algoritma Hasil dan Celah
```
```
Penelitian (Gap)
```
Wijaya et al.
```
(2024)
```
Analisis pola spasial
data gempa di
Sulawesi
```
(2019-2023).
```
DBSCAN Clustering. Mampu mendeteksi
pola klaster dan noise
spasial. Kelemahan:
Ruang lingkup
komputasi terbatas
pada level satu pulau.
Morante-Carballo Systematic review Tinjauan Literatur Menggarisbawahi
8
```
Penulis (Tahun) Topik Penelitian Metode/Algoritma Hasil dan Celah
```
```
Penelitian (Gap)
```
```
et al. (2024) atas ancaman hazard
```
pesisir Cincin Api
Pasifik.
Sistematis. urgensi mitigasi lintas
negara secara
terpadu. Kelemahan,
Masih bersifat
kualitatif dan tanpa
intervensi model
algoritma prediktif.
PUSPITA, D. D., et
```
al. (2025)
```
Random Forest
Analysis for
Predicting the
Probability of
Earthquake in
Indonesia
Random Forest. Model prediksi
mencapai performa
akurasi klasifikasi
90,78%. Kelemahan:
Belum diintegrasikan
dengan
pra-pemrosesan peta
```
spasial (clustering
```
```
awal).
```
SALEEM,
Muhammad Asim,
```
et al. (2025)
```
Neural-XGBoost: A
hybrid approach for
disaster prediction
and management
using machine
learning
```
Jaringan Saraf (Neural
```
```
Network), XGBoost,
```
SMOTE.
F1-Score yang unggul
sebesar 0,96 dalam
mendeteksi pola
risiko gempa.
```
Kelemahan: Analisis
```
tidak secara spesifik
mengeksploitasi
arsitektur pergerakan
seismik Asia.
Penelitian Ini
```
(2026)
```
Analisis dan
Klasifikasi Zona
Hazard Gempa Di
Asia Berbasis Big
Data Pipeline.
HDBSCAN, Random
Forest, XGBoost, Big
Data Pipeline.
Mengekstraksi fitur
spasial geografis
melalui klastering
densitas untuk
meningkatkan
kemampuan Random
Forest & XGBoost
pada orkestrasi
infrastruktur Big Data.
2.2 Analisis Geospasial dan Data Spasial
Fenomena pergerakan lempeng tektonik yang melepaskan tegangan mekanis
dalam bentuk gempa bumi secara inheren terikat erat dengan dimensi geografis
keruangan. Analisis geospasial dalam konteks seismologi modern mensyaratkan
translasi fenomena fisik ini ke dalam komponen data tabular, di mana observasi
kegempaan direpresentasikan ke dalam atribut fundamental berupa koordinat
```
geografis (latitude lintang dan longitude bujur), serta metrik geofisika seperti
```
```
metrik kedalaman hiposentrum (depth) dan besaran lepasan energi seismik
```
```
(magnitude). Penafsiran relasi antar variabel koordinat lintas negara ini
```
9
mengungkap pembentukan koridor episentrum yang mengonfirmasi eksistensi
```
patahan (faults) makro.
```
Sejalan dengan tuntutan pengolahan data regional yang mengumpulkan
rekaman historis puluhan tahun dari lembaga global, ukuran dataset tidak lagi
kompatibel dengan teknologi preprocessing konvensional. Transformasi metadata
berkecepatan tinggi dengan anomali tak beraturan membutuhkan dukungan
kerangka kerja arsitektur Big Data Pipeline. Secara akademis, arsitektur Big Data
Pipeline merupakan sebuah ekosistem pemrosesan otonom yang merangkai
```
aktivitas penyerapan (data ingestion), penyaringan kualitas, transformasi spasial,
```
```
agregasi, hingga pemuatan menuju titik penyimpanan (Data Warehouse
```
```
relasional). Implementasi pipeline data memastikan penyerapan file berformat
```
semiterstruktur seperti format GeoJSON yang dipanggil melalui Application
```
Programming Interface (API) secara otomatis dikonversi dan dibersihkan dari
```
```
duplikasi, agar variabel-variabel keruangan (spatial geometries) siap diinjeksikan
```
secara deterministik ke dalam simulasi machine learning selanjutnya.
```
2.3 Clustering (HDBSCAN)
```
Salah satu limitasi esensial dalam menganalisis probabilitas zonasi gempa
bumi secara regional adalah kecenderungan penyebaran episentrum yang tidak
```
beraturan serta memiliki disparitas tingkat kepadatan (varying densities) di
```
berbagai titik lempeng tektonik. Gempa bumi secara spasial membentuk klaster
patahan dengan kepadatan gempa yang berbeda-beda satu sama lain, diwarnai
banyak intervensi sinyal acak atau mikro seismisitas ireguler. Algoritma DBSCAN
```
konvensional memaksa klasterisasi menggunakan batasan jarak global ( ) yangϵ
```
tunggal, sehingga kerap kali menggabungkan atau justru memecah klaster yang
memiliki kepadatan yang bervariasi. Sebagai solusi fundamental, implementasi
analitik difokuskan pada pendekatan algoritma kepadatan hierarkis, yakni
Hierarchical Density-Based Spatial Clustering of Applications with Noise
```
(HDBSCAN).
```
Gambar 2.1 Ilustrasi Ketangguhan HDBSCAN Terhadap Variasi Kepadatan Data
```
(Varying Densities)
```
10
Berdasarkan Gambar 2.1, terlihat dengan jelas bahwa HDBSCAN sangat
```
tangguh (robust) dalam mengidentifikasi klaster-klaster yang memiliki tingkat
```
kerapatan yang berbeda-beda secara simultan. Algoritma ini tidak hanya berhasil
memisahkan klaster padat dan klaster renggang dengan akurat, tetapi juga secara
cerdas mengeksklusi titik-titik data yang tersebar secara acak sebagai pencilan
```
atau noise (direpresentasikan oleh titik-titik abu-abu).
```
Algoritma HDBSCAN, yang dikembangkan oleh Campello, Moulavi, dan
```
Sander (2013), secara komprehensif memperluas DBSCAN dengan
```
mengonversinya menjadi algoritma hierarchical clustering, sebelum
```
mengekstraksi kelompok secara datar (flat clustering) berdasarkan pada indikator
```
stabilitas klaster tertinggi yang ditemukannya.
Aksioma matematika HDBSCAN berlandaskan pada transformasi ruang jarak
```
untuk mengeksklusi secara efektif area-area patahan palsu (sparse noise).
```
Pertama, perhitungan kepadatan dievaluasi melalui penentuan titik inti, dengan
```
mengukur Core Distance atau Jarak Inti ( ), yang secara matematis𝑐𝑜𝑟𝑒𝑘(𝑥)
```
```
didefinisikan sebagai jarak absolut dari suatu titik data (episentrum gempa) ke𝑥
```
titik tetangga terdekat ke- -nya.𝑘
Untuk memperkuat ketahanan terhadap noise, parameter jarak Euclidean
```
linier konvensional tidak lagi digunakan secara langsung, melainkan𝑑(𝑎, 𝑏)
```
```
digantikan dengan Jarak Keterjangkauan Timbal-Balik (Mutual Reachability
```
```
Distance). Metrik matematis ini menjamin bahwa dua titik dengan kerapatan
```
```
tinggi akan digabungkan, sedangkan titik-titik yang terindikasi anomali (jarak
```
```
intinya terlalu jauh) akan didorong keluar. Formulasinya adalah sebagai berikut:
```
```
(2.1)𝑑𝑚𝑟𝑒𝑎𝑐ℎ−𝑘(𝑎, 𝑏) = 𝑚𝑎𝑥(𝑐𝑜𝑟𝑒𝑘(𝑎), 𝑐𝑜𝑟𝑒𝑘(𝑏), 𝑑(𝑎, 𝑏))
```
Berdasarkan transformasi matriks dari persamaan Mutual Reachability
Distance di atas, algoritma HDBSCAN selanjutnya akan mengkonstruksi sebuah
```
Minimum Spanning Tree (Pohon Rentang Minimum) untuk mengurutkan titik
```
episentrum, mendeteksi struktur kepadatan alami, dan memotong cabang graf
```
secara komputasional menjadi label-label klaster stabil (koridor patahan utama)
```
```
dan mengeksklusi titik-titik lemah sebagai derau murni (background noise).
```
Kemampuan seleksi tingkat tinggi inilah yang membuat varian ini dominan di
domain geofisika.
```
2.4 Klasifikasi (Random Forest and XGBoost)
```
Kendati DBSCAN sanggup membelah geografi benua ke dalam label-label
klaster kawasan patahan berdensitas tinggi, metode klastering tersebut pada
hakikatnya bersifat buta terhadap intensitas probabilitas masa depan dari klaster
```
itu sendiri. Proses penentuan target pelabelan risiko hazard (kategori “Low”,
```
```
“Medium”, “High”, atau “Very High”) perlu ditransformasikan menjadi tugas
```
algoritma klasifikasi supervised machine learning. Evaluasi kinerja komputasi
dalam penelitian ini dibebankan pada dua algoritma raksasa berbasis ansambel
11
```
pohon terdistribusi: Random Forest (RF) dan Extreme Gradient Boosting
```
```
(XGBoost).
```
2.4.1 Random Forest
Konstruksi arsitektur algoritma Random Forest bekerja dengan cara
menginisiasi paralelisme pembentukan ribuan pohon keputusan mandiri
menggunakan partisi sampel data acak dengan teknik agregasi bootstrap
```
(bagging), yang mana hasil determinasi prediksi klasifikasi terakhir dicapai
```
melalui mekanisme musyawarah agregat majority voting terbanyak dari seluruh
```
struktur pohon. Untuk menghitung keandalan pemisahan percabangan (node
```
```
splitting) ke bawah pada masing-masing pohon di dimensi variabel geofisik
```
```
(seperti kedalaman hiposentrum dan ukuran magnitude), RF secara matematis
```
```
harus mengevaluasi ketidakmurnian distribusi klasifikasi (impurity) pada sebuah
```
simpul data. Fungsi kemurnian umumnya dieksekusi menggunakan Indeks Gini
```
(Gini Impurity), yang mengukur derajat probabilitas elemen klasifikasi gempa
```
yang ditempatkan ke dalam cabang yang salah berdasarkan proporsi distribusi
kelas secara acak, dengan fungsi sebagai berikut:
```
(2.2)𝐺𝑖𝑛𝑖 = 1 −
```
𝑖=1
𝐶
∑ 𝑝𝑖2
Di dalam matriks formulasi ini, representasi dari simbol merujuk kepada𝐶
total gabungan seluruh kategori kelas hazard klasifikasi, sedangkan koefisien 𝑝𝑖
menerjemahkan estimasi rasio dari frekuensi kelas target tipe terhadap𝑖
```
himpunan sampel gempa dalam satu cabang titik pisah (node). Sebagai model
```
optimasi pemisahan selain Gini, algoritma pohon juga dapat memaksimumkan
```
Nilai Perolehan Informasi (Information Gain) yang berbanding terbalik secara
```
linier terhadap kerugian Entropy Shannon, dengan parameterisasi persamaan
fungsi diferensial:
```
(2.3)𝐸𝑛𝑡𝑟𝑜𝑝𝑦 =−
```
𝑖=1
𝐶
```
∑ 𝑝𝑖𝑙𝑜𝑔2(𝑝𝑖)
```
```
2.4.2 Extreme Gradient Boosting (XGBoost)
```
Berkebalikan arah dengan mekanisme pemisahan Random Forest yang
```
bergerak bebas serentak secara linier independen (paralel), fungsionalitas
```
algoritma XGBoost mengadopsi mekanisme Gradient Boosting yaitu pembuahan
iteratif sekuensial. Fungsi arsitekturnya difokuskan untuk memastikan
keberhasilan struktur pohon ke- dalam menetralkan dan mengompensasi indeks𝑡
```
persentase eror (residual estimation error) yang diakumulasi secara historis oleh
```
silsilah prediksi pohon sebelumnya. Optimasi gradien fungsional dicapai dengan
```
menargetkan minimalisasi nilai secara agresif terhadap fungsi objektif (Objective
```
```
Function):
```
```
(2.4)𝑂𝑏𝑗 =
```
𝑖=1
𝑛
```
∑ 𝐿(𝑦𝑖, 𝑦^𝑖) +
```
𝑘=1
𝐾
```
∑ Ω(𝑓𝑘)
```
12
```
Elemen taksiran pertama merupakan instrumen formulasi∑ 𝐿(𝑦𝑖, 𝑦
```
^
```
𝑖)
```
Training Loss yang menaksir total agregat nilai defiasi galat antara probabilitas
keparahan gempa prediktif hasil rekayasa XGBoost dihadapkan dengan𝑦
^
𝑖
kebenaran lapangan aktual . Adapun elemen partisi taksiran kompensasi aditif𝑦𝑖
```
adalah suku regularisasi ekuilibrium yang diinjeksikan secara sadar olehΩ(𝑓𝑘)
```
arsitek model demi mengebiri peluang terjadinya overfitting dan membatasi
ukuran perluasan kompleksitas fungsional daun pada setiap partisi ke- . Demi𝑘
memuluskan orkestrasi pemrosesan pada data besar tanpa terjebak kendala
fungsi loss kustom yang non-diferensiabel di dalam dimensi Euclidean, mesin
XGBoost memanfaatkan teknik pintasan pendekatan fungsi aproksimasi Deret
Taylor orde kedua untuk melakukan dekomposisi pada variabel objektif iterasi
pembelajar ke- :𝑡
```
(2.5)𝑂𝑏𝑗(𝑡) ≈
```
𝑖=1
𝑛
```
∑ [𝐿(𝑦𝑖, 𝑦^𝑖
```
```
(𝑡−1)
```
```
) + 𝑔𝑖𝑓𝑡(𝑥𝑖) + 12 ℎ𝑖𝑓𝑖2(𝑥𝑖)] + Ω(𝑓𝑡)
```
Dalam manipulasi kalkulus deret Taylor di atas, nilai konstanta numeric 𝑔𝑖
```
merupakan perwujudan simbol dari turunan parsial pertama (Gradient) terhadap
```
galat loss function, sementara koefisien akselerasi fisis mengonversi nilai dariℎ𝑖
```
derivasi turunan kedua (Hessian) yang menangkap akselerasi kelengkungan laju
```
```
error. Sifat komputasional berbasis optimasi kuadratik (Hessian gradient descent)
```
inilah yang secara konklusif mengukuhkan XGBoost sebagai salah satu arsitektur
pengklasifikasi kecerdasan buatan paling mutakhir dan efisien dalam
mengklasifikasi taksonomi data gempa bumi berdimensi besar.
2.5 Evaluasi Kinerja Model Klasifikasi
Dalam domain supervised machine learning, efikasi sebuah algoritma
klasifikasi dievaluasi secara kuantitatif dengan merepresentasikan hasil
```
prediksinya ke dalam format Matriks Kekeliruan (Confusion Matrix). Matriks ini
```
```
memetakan perbandingan antara kategori risiko aktual (kebenaran lapangan) dan
```
hasil prediksi yang diberikan model. Pemetaan ini terbagi atas empat kuadran
```
matriks yang krusial, yaitu True Positive (TP), True Negative (TN), False Positive
```
```
(FP), dan False Negative (FN). Mengacu pada konfigurasi nilai-nilai di dalam sel
```
matriks tersebut, formulasi teori metrik evaluasi model dihitung secara
matematis sebagai berikut:
1. Akurasi (Accuracy)
Akurasi merupakan metrik evaluasi paling mendasar yang mengukur proporsi
total tebakan prediksi yang benar dari keseluruhan dataset observasi.
```
(2.6)𝐴𝑐𝑐𝑢𝑟𝑎𝑐𝑦 = 𝑇𝑃+𝑇𝑁𝑇𝑃+𝑇𝑁+𝐹𝑃+𝐹𝑁
```
2. Presisi (Precision)
13
Presisi memformulasikan tingkat ketepatan antara data yang secara spesifik
diprediksi positif oleh model dengan porsi kejadian yang benar-benar positif
di lapangan.
```
(2.7)𝑃𝑟𝑒𝑐𝑖𝑠𝑖𝑜𝑛 = 𝑇𝑃𝑇𝑃+𝐹𝑃
```
3. Sensitvitas (Recall)
Recall berfokus pada probabilitas penemuan yakni menghitung rasio
keberhasilan model dalam mengidentifikasi dan menangkap kembali seluruh
data berkelas positif yang aktual.
```
(2.8)𝑅𝑒𝑐𝑎𝑙𝑙 = 𝑇𝑃𝑇𝑃+𝐹𝑁
```
4. Skor F1 (F1-Score)
F1-Score merupakan indikator ekuivalensi yang merepresentasikan rata-rata
```
harmonik (harmonic mean) antara nilai metrik Presisi dan Recall. Formulasi
```
penyelarasan ini sangat krusial dan diandalkan ketika komputasi berhadapan
dengan asimetri atau ketimpangan yang ekstrem pada distribusi kelas
dataset.
```
(2.9)𝐹1 − 𝑆𝑐𝑜𝑟𝑒 = 2 × 𝑃𝑟𝑒𝑐𝑖𝑠𝑖𝑜𝑛 × 𝑅𝑒𝑐𝑎𝑙𝑙𝑃𝑟𝑒𝑐𝑖𝑠𝑖𝑜𝑛 + 𝑅𝑒𝑐𝑎𝑙𝑙
```
14
BAB 3 METODOLOGI PENELITIAN
3.1 Rancangan Alur Sistem
Gambar 3.1 Rancangan Alur Pelaksanaan Penelitian
Penelitian ini menggunakan pendekatan arsitektur big data pipeline untuk
mengelola alur pengolahan data gempa bumi secara terstruktur dan
```
terotomatisasi. Proses dimulai dari tahap pengumpulan data (data ingestion)
```
yang diperoleh dari sumber terbuka melalui Application Programming Interface
```
(API) United States Geological Survey (USGS) dalam format GeoJSON. Data
```
mentah tersebut kemudian dialirkan ke tahap preprocessing yang meliputi
```
pembersihan data (data cleaning), reduksi duplikasi, serta pengayaan atribut
```
```
spasial (spatial enrichment).
```
Data yang telah diproses kemudian dimuat ke dalam warehouse spasial
```
(PostGIS) untuk memfasilitasi analisis geospasial tingkat lanjut. Tahap analitik
```
pertama adalah implementasi clustering menggunakan metode HDBSCAN untuk
mengidentifikasi pola distribusi dan koridor patahan gempa berdasarkan
kepadatan spasialnya. Guna menjembatani keluaran algoritma unsupervised ini
ke tahap supervised learning, diterapkan tahapan verifikasi kualitas klaster serta
```
penyaringan (noise filtering) terhadap titik-titik anomali. Klaster yang terverifikasi
```
kemudian dianalisis melalui proses risk profiling dan pseudo-labeling untuk
```
merumuskan label target tingkat risiko bencana (seperti Low, Medium, High, dan
```
```
Very High).
```
15
Setelah matriks fitur dan label target terbentuk, tahapan dilanjutkan dengan
```
melakukan pembagian dataset (data splitting) menjadi data latih dan data uji.
```
Proses klasifikasi utama kemudian dieksekusi menggunakan algoritma Random
Forest dan XGBoost untuk memprediksi probabilitas tingkat risiko gempa
berdasarkan karakteristik geofisika dan spasialnya. Model yang telah dilatih
dievaluasi secara komparatif menggunakan metrik accuracy, precision, recall, dan
F1-score. Seluruh rangkaian hasil komputasi dari pipeline ini pada akhirnya
diproyeksikan ke dalam bentuk visualisasi geospasial untuk memudahkan
interpretasi pola distribusi dan tata kelola mitigasi risiko gempa di kawasan Asia.
3.2 Data Recollection dan Preprocessing
Tahap pengumpulan data dijalankan dengan melakukan otomasi
```
pemanggilan Application Programming Interface (API) USGS secara periodik
```
menggunakan sistem penjadwalan Apache Airflow. Ilustrasi data yang
dikumpulkan memiliki karakteristik deret waktu spasial yang berukuran masif
```
(skala Big Data). Data mentah yang diambil berformat GeoJSON, sebuah standar
```
format fail semi-terstruktur yang memuat sekumpulan array dengan struktur
```
properti kunci seperti time (merekam jejak waktu kejadian historis dalam format
```
```
penanda waktu Unix), variabel latitude dan longitude (menyimpan koordinat
```
```
absolut episentrum lintang dan bujur), kedalaman hiposentrum dalam parameter
```
```
lintasan (depth), serta elemen mag (skala besaran magnitudo gempa).
```
Karakteristik kumpulan data seismisitas benua Asia secara historis sangatlah
padat. Sebagai gambaran, dalam wilayah Sulawesi yang dilakukan oleh penelitian
```
Wijaya et al. (2024) saja dapat menghasilkan lebih dari 10.238 record dengan
```
jangka skala 2019 hingga 2023 kejadian tremor signifikan. Oleh karena itu, data
bervolume masif ini kerap diwarnai noise, anomali rekaman, atau nilai yang
kosong akibat kegagalan instrumen seismograf teritorial. Untuk mengatasi
kendala performa tersebut, orkestrasi preprocessing mempekerjakan framework
analitik data tabular kolumnar Polars. Pustaka ini mengeksekusi operasi
pembersihan derau struktural, validasi anomali null, penghapusan duplikasi entri
berulang, serta konversi tipe Unix time menjadi objek datetime secara sangat
cepat di dalam memori. Preprocessing juga memberlakukan filter geofisika secara
eksplisit, dengan membuang tremor magnitude yang tidak masuk akal seperti di
bawah magnitude 0 dan di atas magnitude 10 agar tidak terjadi bias pemetaan
pada aktivitas tektonik tak berdampak. Dataset final yang telah mulus dan
lengkap akan disimpan menjadi artefak fail Parquet sebelum dimuat ke pangkalan
data PostgreSQL berarsitektur PostGIS untuk ekstraksi fitur spasial tingkat lanjut.
3.3 Metode Clustering Spasial
Pada ekosistem penelitian ini, paradigma klasterisasi spasial diubah melalui
penerapan algoritma hierarkis Hierarchical Density-Based Spatial Clustering of
```
Applications with Noise (HDBSCAN). Algoritma ini dimanfaatkan secara khusus
```
untuk mendeteksi koridor-koridor tak kasat mata dari patahan tektonik geologis
16
yang asimetris di seantero geografi benua Asia, berlandaskan perbandingan
kepadatan antar titik dan kestabilannya.
Ekspektasi utama yang diharapkan dari metode berbasis densitas hierarkis ini
adalah kemampuannya yang sangat adaptif dalam menjustifikasi klaster yang
memiliki densitas atau tingkat kerapatan yang berbeda-beda secara alamiah di
dalam data kegempaan aktual. Sebagaimana yang terbukti pada uji penelitian
```
tahap lanjut (seperti observasi pada log pipeline), eksekusi HDBSCAN
```
```
diekspektasikan dapat menemukan koridor linier spesifik (ratusan klaster patahan
```
```
aktif seperti Ring of Fire dan Sunda Megathrust) yang berbentuk serpihan
```
memanjang bukan wilayah membulat. Lebih superior lagi, HDBSCAN dirancang
```
sebagai alat kurasi Signal-to-Noise Ratio (SNR) yang paling tangguh untuk
```
memisahkan sekaligus "membuang" lebih dari separuh data micro seismisitas
```
acak atau sinyal gempa non-struktural sebagai pencilan (noise) secara otonom,
```
tanpa memaksa pengguna memasukkan nilai radius absolut seperti DBSCAN
biasa.
Secara teknis, penemuan label klasterisasi dieksekusi dengan
mengimplementasikan pustaka algoritma HDBSCAN di Python. Peneliti bertugas
melakukan rekayasa optimasi mendalam pada parameter kunci seperti
parameter batas jumlah anggota minimal pembentuk klaster utama
```
(min_cluster_size dengan pengujian awal diset pada nilai representatif seperti
```
```
10) serta batas tetangga kerapatan (min_samples). Titik koordinat yang sangat
```
kuat mengikat konektivitas spasial Mutual Reachability Distance akan menjadi
```
entitas "identitas pelabelan klaster" utama (misal Klaster-0, Klaster-1, hingga
```
```
Klaster-191). Sebaliknya, ribuan data yang terisolasi akan diamputasi sebagai
```
Noise. Kumpulan pelabelan bebas derau inilah yang kemudian dienkapsulasi
```
menjadi fitur analitik spasial murni (engineered feature) menuju modul klasifikasi
```
risiko di hilir arsitektur.
3.4 Penentuan Fitur Analitik Spasial
Pada penelitian ini, proses klasifikasi risiko kebencanaan tidak dapat
dipisahkan dari tahapan klasterisasi spasial yang mendahuluinya. Secara
metodologis, algoritma HDBSCAN yang bersifat unsupervised learning berperan
sebagai instrumen awal untuk mengenali pola kedekatan geometris episentrum
gempa tanpa mengetahui derajat bahaya dari kelompok yang terbentuk. Di sisi
lain, algoritma klasifikasi seperti Random Forest dan XGBoost merupakan model
```
supervised learning yang membutuhkan label target (ground truth) yang jelas
```
sebagai dasar pembelajaran. Guna menjembatani kedua pendekatan tersebut,
tahapan pseudo-labeling atau cluster profiling diimplementasikan untuk
```
menerjemahkan ID klaster numerik menjadi label semantik kategorikal (Risiko
```
```
Low, Medium, High, dan Very High) berdasarkan agregasi karakteristik geofisika di
```
dalam klaster tersebut. Tanpa adanya tahapan bridging ini, model klasifikasi tidak
```
akan memiliki variabel target ( ) yang valid untuk diprediksi.𝑦
```
Dalam membangun arsitektur model klasifikasi yang akurat, penentuan
```
matriks fitur atau Fitur ( ) memegang peranan sebagai variabel prediktor atau𝑋
```
17
```
himpunan petunjuk utama yang akan dievaluasi oleh model. Fitur ( ) dalam𝑋
```
konteks penelitian ini merupakan representasi parameter fisik dan spasial dari
setiap kejadian gempa bumi seperti nilai magnitudo, tingkat kedalaman
```
hiposentrum, serta variabel turunan hasil rekayasa data pipeline (engineered
```
```
features) seperti kepadatan kejadian dalam radius tertentu dan jarak ke kejadian
```
terdekat. Model klasifikasi akan menganalisis korelasi non-linear dari seluruh
```
dimensi Fitur ( ) ini untuk mempelajari pola bagaimana karakteristik fisik sebuah𝑋
```
gempa dapat mendorong entitas data tersebut diklasifikasikan ke dalam kelas
```
target risiko ( ) tertentu. Penyeleksian Fitur ( ) yang representatif menjamin𝑦 𝑋
```
bahwa model klasifikasi memiliki bahan baku informasi yang kaya untuk
meminimalkan margin galat saat memprediksi tingkatan risiko pada data
kegempaan yang baru.
3.5 Metode Klasifikasi Risiko
Bila tahapan DBSCAN hanya membatasi temuan pada demarkasi koridor
```
wilayah (mana yang padat gempa dan mana yang tidak), maka metode klasifikasi
```
supervised machine learning disini difungsikan secara agresif untuk
mengonstruksikan "seberapa letal/berisiko" taksonomi bahaya pada koordinat
wilayah tersebut ke depannya. Ekspektasi utama dari penerapan tahapan ini
adalah untuk memenangkan perlawanan komputasi terhadap dataset
```
kebencanaan geomorfologi yang secara alamiah sangat tidak seimbang (highly
```
```
imbalanced), karena fenomena gempa yang merusak (kategori tinggi)
```
populasinya secara statistik sangat langka ketimbang kejadian gempa getaran
```
minor (kategori rendah).
```
```
Untuk menemukan klasifikasi hasil prediksi risiko (yakni label target tipe:
```
```
Risiko Low, Medium, High, atau Very High), penelitian ini mengadopsi kompetisi
```
teknis antara dua arsitektur ensambel modern:
1. Random Forest: Diimplementasikan dengan mengakses kelas model
sklearn.ensemble.RandomForestClassifier dari pustaka Scikit-learn. Algoritma
ini mencari model prediksi risiko gempa dengan membangun paralelisme
ribuan pohon keputusan yang dilatih pada irisan-irisan data terpisah
```
menggunakan kombinasi acak (bootstrap bagging). Penentuan tingkat
```
risikonya ditetapkan dari mekanisme pemungutan suara terbanyak dari
keseluruhan pohon yang dijamin sangat stabil menghindari masalah
overfitting.
2. XGBoost: Dilakukan melalui penginisiasian pustaka eksternal Extreme
```
Gradient Boosting (XGBoost). Pencarian model prediksi di sini tidak berjalan
```
berbarengan, melainkan dieksekusi secara iteratif dan korektif, di mana
pohon klasifikasi yang baru secara agresif belajar dari galat nilai error
```
(gradien) pendahulunya. Algoritma ini diekspektasikan menjadi spesialis
```
terkuat dalam menangani klasifikasi minoritas bahaya gempa pada area
perbatasan.
18
Setiap model pohon akan disuplai oleh seluruh fitur variabel matang yang
telah terintegrasi seperti metrik logaritma kedalaman, metrik magnitude, beserta
```
penambahan kolom identitas klaster densitas DBSCAN (cluster_ID), guna
```
melakukan taksiran evaluasi tingkat probabilitas hazard berskala transnasional.
3.6 Evaluasi Model
Sesuai dengan komentar dosen pembimbing agar tidak memasukkan definisi
```
teoritis metrik evaluasi secara langsung ke subbab ini (yang telah diposisikan di
```
```
Subbab 2.5 Landasan Kepustakaan), evaluasi model pada bagian Metodologi ini
```
difokuskan penuh pada kontekstualisasinya terhadap realitas permasalahan
mitigasi bencana kegempaan.
Kinerja algoritma klasifikasi Random Forest maupun XGBoost diekstraksi dari
```
perhitungan Matriks Kekeliruan (Confusion Matrix), dengan interpretasi
```
penerapan metrik sebagai berikut:
1. Akurasi: Diaplikasikan untuk mengukur dan memberikan angka persentase
mengenai seberapa tepat keseluruhan arsitektur kecerdasan buatan mampu
```
menempatkan tebakan label zona risiko (“Low”, “Medium”, “High”, “Very
```
```
High”) ke setiap titik episentrum benua Asia secara tepat sasaran, jika
```
ditandingkan dengan fakta dari lembaga seismik.
2. Presisi: Berfungsi krusial sebagai filter pencegah "Alarm Palsu" mitigasi (False
```
Positives). Dalam skenario pengambilan keputusan anggaran bencana
```
pemerintah regional, presisi yang tinggi menjadi jaminan bahwa sebuah
kota/wilayah yang diklasifikasikan oleh mesin berstatus 'Zona Hazard
Berbahaya' memang secara historis memiliki catatan riil potensi bahaya fatal
```
tersebut, sehingga alokasi dana perkuatan bangunan (retrofitting) tidak
```
terbuang sia-sia ke wilayah yang sejatinya damai.
3. Recall (Sensitivitas): Berperan paling vital dari sisi perlindungan nyawa
```
penduduk (life-safety parameter). Ekspektasi evaluasi ini adalah menjamin
```
seminimal mungkin kegagalan sistem dalam menangkap zona bahaya aktual
```
(False Negatives). Angka Recall yang mendekati sempurna membuktikan
```
bahwa algoritma klasifikasi XGBoost atau RF tidak pernah "kelewatan" dalam
mendeteksi dan memberi flag kepada kantong wilayah penyimpan tegangan
gempa yang letal.
4. F1-Score: Diterapkan secara khusus untuk membuktikan stabilitas
keseimbangan komputasi prediktif Random Forest dan XGBoost karena sifat
asimetri data bencana gempa. Indikator ini memastikan bahwa ketepatan
komputasi dalam mengenali pola rekahan bencana skala raksasa, sama lincah
dan berimbangnya dengan ketepatan komputasi saat ia menengarai ratusan
ribu riwayat gempa getaran minor.
19
BAB 4 PERANCANGAN & IMPLEMENTASI
4.1 Perancangan Sistem dan Arsitektur Big Data Pipeline
Desain arsitektur perangkat lunak untuk analisis hazard seismik regional ini
dirancang bertumpu pada prinsip pemisahan ruang fungsional data dan
pemrosesan terdistribusi. Sistem ini mengadopsi empat pilar teknologi mutakhir
yang diintegrasikan untuk beroperasi secara asinkron. Apache Airflow dirancang
sebagai otak orkestrator sentral yang mendikte alur tugas melalui struktur
```
eksekusi Directed Acyclic Graph (DAG). Proses transformasi aljabar dan
```
pembersihan data bervolume masif dirancang sedemikian rupa agar menghindari
limitasi hambatan memori eksekutor lokal melalui penerapan pustaka komputasi
Polars, yang secara inheren mengutilisasi pemrosesan multithreading berbasis
kolom.
Untuk memastikan redundansi keamanan dan keandalan data dari ancaman
degradasi komputasi, perancangan sistem menetapkan dua lapisan persistensi
penyimpanan paralel. Penyimpanan objek termutakhir berbasis MinIO untuk
pelestarian pengarsipan cadangan file terkompresi format Parquet sebagai Data
```
Lake (Danau Data), dan sistem manajemen basis data PostgreSQL yang diperkuat
```
```
oleh ekstensi fungsionalitas PostGIS sebagai Data Warehouse (Gudang Data) yang
```
```
berfungsi melayani analisis kueri keruangan secara seketika (real-time).
```
Perancangan transmisi komunikasi lintas node diatur menggunakan kapiler jalur
```
data Cross-Communication (XCom) pada orkestrator, yang bertugas memastikan
```
kelengkapan validasi proses hulu tuntas dieksekusi sebelum sistem mengaktifkan
modul kecerdasan buatan di sisi hilir.
4.2 Perancangan Skema Pangkalan Data dan Fitur Keruangan
```
(Geospatial Feature Engineering)
```
Guna memfasilitasi algoritma komputasi klasifikasi dan penemuan klaster
agar dapat memproses topologi geografis permukaan sferis secara matematis,
perancangan skema relasional basis data memegang determinasi mutlak. Tabel
relasional utama yang dinamakan earthquakes dirancang tidak sekadar untuk
menerima injeksi tipe bilangan desimal standar primitif untuk mencatat kordinat,
melainkan dirancang secara radikal untuk menggunakan struktur entitas
```
geometri spasial GEOMETRY (Point, 4326) khas kapabilitas PostGIS. Desain tipe
```
data khusus ini secara otomatis mengindeks lokasi riwayat gempa dengan
merujuk langsung pada sistem referensi geodesi elipsoid standar bumi.
Melampaui batasan penampungan parameter dasar geofisika seperti metrik
```
kekuatan pelepasan tegangan energi (magnitude) dan metrik tingkat kedalaman
```
```
pusat patahan (depth_km), arsitektur sistem memberikan mandat perancangan
```
untuk merumuskan setidaknya enam fitur atribut spasial artifisial. Fitur rekayasa
keruangan ini didesain guna memperluas horizon dimensi pengenalan bagi
algoritma pembelajar. Fokus utama terletak pada desain kalkulasi jarak absolut
```
terhadap gempa bertetangga terdekat (nearest_event_km) serta formulasi indeks
```
20
komposit kepadatan deformasi retakan radius keruangan seratus kilometer
```
(event_density_100km). Formulasi metrik fitur turunan ini dirancang
```
perhitungannya dengan mengakomodasi algoritma trigonometri transformasi
berbasis jarak Haversine, yang krusial untuk menjamin tingkat akurasi komputasi
keruangan pada model permukaan melengkung. Ketentuan arsitektur data
matriks ini ditetapkan secara mutlak melalui validasi schema-on-write guna
memberikan garansi proteksi dari infiltrasi entri derau kosong yang dapat
menghambat operasi model prediksi risiko spasial.
4.3 Perancangan Model Analitik Geospasial dan Prediksi Risiko
Bahaya
Desain kerangka pemodelan fungsional kecerdasan buatan dibedah menjadi
dua topologi pilar analitik terpisah: model penemuan pola fitur spasial nir-label
dan model klasifikasi kategorisasi risiko terawasi.
Pada fase ekstraksi delineasi klasterisasi keruangan, algoritma Hierarchical
```
Density-Based Spatial Clustering of Applications with Noise (HDBSCAN) dirancang
```
parameternya secara spesifik dengan batasan kendali min_cluster_size = 10.
Spesifikasi hiperparameter arsitektural ini dirumuskan berlandaskan asumsi
heuristik seismologi, yang secara komputasi memaksa enjin algoritmik untuk
mengabaikan getaran pergerakan lempeng sporadis mikro, dan semata-mata
mengonstruksi pembentukan entitas teritorial koridor patahan tektonik makro
apabila berhasil dijumpai manifestasi kohesi sepuluh atau lebih titik kepadatan
observasi yang memenuhi kriteria Mutual Reachability Distance. Sisa kumpulan
titik kordinat yang lemah sebarannya secara desain operasional tidak diamputasi,
melainkan diinstruksikan oleh perancang untuk direklasifikasi sebagai barisan
```
entitas anomali pelengkap beridentitas label konstan Cluster_ID = -1 (Noise),
```
sebagai antisipasi pelestarian memori aktivitas gempa letal intra-lempeng.
Memasuki arena topologi klasifikasi penentuan batas risiko, arsitektur
machine learning dienkapsulasi menggunakan paradigma Object-Oriented
Programming ke dalam suatu modul konseptual pengklasifikasi. Mengingat
distribusi taksonomi inventaris kelas keparahan bencana kegempaan sangatlah
asimetris di mana kemunculan persentase gempa katastropik jauh lebih tertindas
populasinya dibanding dominasi getaran getaran minor desain perancangan
memandatkan pemisahan paksa pada partisi matriks data. Strategi perancangan
ini memaksakan pengadopsian teknik stratifikasi absolut dengan irisan porsi
```
80:20 (stratified split) yang secara definitif mencegah degradasi akurasi saat
```
mendeteksi krisis minoritas ekstrem.
Sistem model komputasi dirancang untuk mempertandingkan optimasi
matematika dari dua arsitektur pohon ansambel berskala tinggi:
1. Random Forest: Didesain bertindak sebagai penjaga garis batas pangkalan
atau parameter performa baseline, dengan hiperparameter penciptaan
```
n_estimators = 100 agregasi percabangan fungsi pohon prediktif terdistribusi
```
paralel, berserta limitasi pendalaman fungsi struktur pada max_depth = 15.
21
Desain parameter matematis ini mengevaluasi persentase minimisasi metrik
nilai pengotor Gini Impurity dalam mendiktekan determinasi penentuan
setiap percabangan kelas tebakan.
2. XGBoost: Arsitektur kelas mutakhir ini secara filosofis tidak dirancang
beroperasi paralel bebas serentak, melainkan dibentuk untuk bergerak
sekuensial secara metodis dalam memperbaiki kalkulasi kesalahan berulang
```
(residual errors) yang ditinggalkan dari pohon perintisnya, memanfaatkan
```
```
diferensial kalkulus turunan optimasi proyektif orde kedua (Hessian).
```
Kerangka arsitekturalnya dikekang penyebarannya dengan membatasi
kedalaman simpul daun maksimal pada parameter max_depth = 7, diikat
dengan optimasi fungsional parameter metrik pendeteksi kesalahan
asimetris multiclass logloss, serta penegakan penalti regulerisasi internal
```
(L1/L2). Keputusan desain integratif ini ditujukan khusus guna meredam
```
```
potensi komputasi hafalan keliru (overfitting) sekaligus mendongkrak
```
ketajaman akurasi pada lapisan klaster data berpopulasi paling kerdil
```
(probabilitas zona gempa kategori mematikan).
```
4.4 Implementasi Sistem Komputasi dan Validasi Hasil Model
4.4.1 Recollection Dan Preprocessing Data
```
Proses pengumpulan data (data ingestion) dari United States Geological
```
```
Survey (USGS) dan pra-pemrosesan data dijalankan secara terotomatisasi
```
menggunakan orkestrasi Apache Airflow. Rangkaian aktivitas ini dieksekusi
```
melalui antarmuka Directed Acyclic Graph (DAG). Gambar 4.1 menunjukkan log
```
```
eksekusi proses ingestion data mentah (raw data).
```
22
Gambar 4.1 Log Eksekusi Raw Data Ingestion Pada Apache Airflow
Berdasarkan Gambar 4.1, proses penarikan data via API berhasil
mengumpulkan sebanyak 11.420 rekaman data kejadian gempa unik di wilayah
Asia. Data yang terhimpun mencakup rentang waktu dari 20 Mei 2024 hingga 19
Mei 2026. Dari rangkuman data mentah tersebut, didapatkan magnitudo gempa
yang berkisar antara 3,0 hingga 7,7 Skala Richter, dengan rata-rata kedalaman
hiposentrum sebesar 73,1 km. Data mentah ini kemudian disimpan ke dalam
format .parquet untuk efisiensi penyimpanan big data sebesar 0,64 MB.
```
Selanjutnya, tahapan pra-pemrosesan (preprocessing) data dieksekusi
```
menggunakan pustaka Polars in-memory. Proses ini tidak hanya melakukan
```
penambahan fitur (enrichment), tetapi juga diawali dengan pembersihan data
```
```
(data cleaning) dan reduksi data melalui proses deduplikasi. Gambar 4.2
```
menyajikan log eksekusi tahap cleaning dan reduksi data tersebut.
23
Gambar 4.2 Log Eksekusi Proses Cleaning dan Reduction Data Pada Apache
Airflow
```
Berdasarkan rekaman log pada Gambar 4.2, pada langkah ke-2 (Data
```
```
cleaning) sistem secara otomatis memindai anomali nilai kosong pada atribut
```
lokasi. Nilai place='None' atau NULL berhasil divalidasi dan di standardisasi
menjadi label 'Unknown Location', sehingga integritas struktur data tetap
```
terjaga dengan total 11.420 rekaman. Pada langkah ke-3 (Deduplication), proses
```
```
reduksi data menyeleksi duplikasi identitas kejadian (ID gempa) dan mengurutkan
```
```
data berdasarkan waktu kejadian terbaru (newest first). Karena data API USGS
```
sudah cukup bersih dari asal sumbernya, tidak ditemukan adanya duplikat pada
penarikan batch ini, sehingga menyisakan 11.420 rekaman unik yang solid.
Setelah data dipastikan bersih, sistem melanjutkan ke tahap rekayasa fitur
```
geospasial (spatial feature engineering). Gambar 4.3 menampilkan log
```
```
penyelesaian tahap enrichment spasial dan pemuatan (load) data ke dalam
```
warehouse atau database PostGIS.
24
Gambar 4.3 Log Eksekusi Proses Enrichment Spasial dan Pemuatan Data
Pada Apache Airflow
Sesuai dengan Gambar 4.3, dataset diperkaya dengan penambahan 6 fitur
```
spasial baru, di antaranya jarak kejadian terdekat (nearest_event_km), kepadatan
```
```
kejadian dalam radius 100 km (event_density_100km), zona seismik
```
```
(seismic_zone), identitas sel grid (grid_cell_id), jarak titik pusat
```
```
(centroid_distance_km), dan skor risiko spasial (spatial_risk_score). Data hasil
```
preprocessing akhir ini berukuran 0,79 MB dan sukses diunggah ke media
penyimpanan objek MinIO sebagai backup data, serta disimpan ke warehouse
```
data relasional PostgreSQL (PostGIS) untuk digunakan pada tahap clustering dan
```
klasifikasi tingkat lanjut.
4.4.2 Spatial Clustering HDBSCAN
Tahap implementasi clustering dilakukan menggunakan modul hdbscan
berbasis bahasa pemrograman Python yang dienkapsulasi di dalam orkestrasi
Apache Airflow melalui task earthquake_clustering. Pada tahap ini, algoritma
```
menerima masukan berupa matriks fitur koordinat spasial (latitude dan
```
```
longitude) yang telah dibersihkan dan tersimpan di dalam PostGIS pada tahap
```
preprocessing sebelumnya.
Dalam penulisan kodenya, parameter krusial yang diatur secara eksplisit
adalah min_cluster_size = 10. Parameter ini diatur untuk menginstruksikan
algoritma agar hanya membentuk sebuah struktur klaster patahan jika setidaknya
terdapat 10 kejadian gempa yang saling berdekatan di dalam ruang densitas
geografis. Titik-titik yang gagal memenuhi ambang batas kepadatan ini akan
```
otomatis dibuang ke dalam kategori -1 (Noise).
```
25
Gambar 4.4 Log Eksekusi Proses Clustering HDBSCAN Pada Apache Airflow
Berdasarkan Gambar 4.4, log eksekusi komputasi menunjukkan bahwa
```
implementasi algoritma HDBSCAN berjalan dengan sukses (status eksekusi
```
```
terekam pada tahap Step 6, Running HDBSCAN clustering). Dari total 11.403
```
rekaman data yang diproses, sistem komputasi berhasil mengekstraksi 194
klaster patahan seismik di seluruh wilayah Asia. Bersamaan dengan itu, HDBSCAN
mengeksekusi parameter densitasnya dengan ketat sehingga memisahkan 5.917
```
titik kejadian sebagai noise (gempa acak/latar).
```
Log eksekusi juga memvalidasi bahwa sistem mampu melakukan enumerasi
distribusi kepadatan secara spesifik ke dalam masing-masing array klaster,
sebagai contoh Klaster 0 menampung 90 titik gempa, Klaster 1 menampung 23
```
titik, dan seterusnya. Identitas klaster (Cluster_ID) yang berhasil diekstraksi inilah
```
yang kemudian digabungkan ke dalam matriks fitur utama pangkalan data, untuk
selanjutnya diteruskan sebagai atribut pendukung bagi algoritma klasifikasi
XGBoost dan Random Forest pada tahap berikutnya.
```
4.4.3 Klasifikasi Risiko Zona Bahaya (Hazard Zone Classification)
```
Fase klasifikasi risiko seismik merupakan tahapan transisi fungsional dari
```
pemodelan geospasial tak-terawasi (unsupervised learning) menuju klasifikasi
```
```
prediktif terawasi (supervised learning). Algoritma klasterisasi spasial HDBSCAN
```
sebelumnya telah berhasil mendemarkasi koridor patahan tektonik, namun
secara ontologis belum memiliki fungsi objektif untuk menakar derajat keparahan
26
ancaman di masa depan. Oleh karena itu, arsitektur prediktif dibangun untuk
mengintegrasikan luaran klaster tersebut dengan fitur geofisika empiris guna
memprediksi kelas probabilitas ancaman gempa menggunakan metodologi
ensemble trees, yang secara saintifik terbukti amat tangguh dalam memproses
volume data kebencanaan yang asimetris dan masif.
4.4.3.1 Arsitektur Klasifikasi Berbasis Supervised Learning
Kerangka arsitektur klasifikasi dikonstruksi dengan mengomparasikan dua
algoritma supremasi pembelajaran ansambel berbasis pohon keputusan.
```
Algoritma Random Forest diterapkan sebagai model basis (baseline classifier).
```
```
Algoritma ini mendayagunakan teknik agregasi bootstrap (bagging) guna melatih
```
ratusan pohon keputusan independen secara paralel pada subsampel data acak.
```
Mekanisme pemungutan suara terbanyak (majority voting) dalam Random Forest
```
terbukti secara esensial meminimalisasi varians komputasi serta mencegah
fenomena overfitting tatkala menemui ketimpangan representasi kelas bencana.
Hiperparameter dioptimasi secara spesifik dengan membatasi jumlah estimasi
maksimum pada 100 pohon pembentuk dan kedalaman pohon pada ambang
batas 15 tingkat percabangan agar tidak terjadi penghafalan derau spasial.
Sebagai arsitektur komparator utama, algoritma Extreme Gradient Boosting
```
(XGBoost) diimplementasikan untuk mengompensasi limitasi paralelisme.
```
Berlawanan dengan metode bagging, XGBoost menerapkan metode
```
pendongkrakan berurutan (sequential boosting), di mana fungsi pembelajaran
```
```
difokuskan secara iteratif untuk mengoreksi residu galat (residual errors) dari
```
prediksi pohon pendahulunya menggunakan optimasi penurunan gradien
```
(gradient descent optimization). Keunggulan fundamental XGBoost terletak pada
```
integrasi regularisasi fungsional L1 dan L2 secara inheren, memungkinkannya
mengendalikan overfitting seraya menjaga tingkat akurasi presisi pada deteksi
kelas kejadian ekstrem yang populasinya sangat langka. Model ini
```
diparameterisasi dengan kedalaman yang lebih dangkal (7 tingkat percabangan)
```
serta dikendalikan oleh metrik multiclass logloss untuk menangani sebaran fungsi
distribusi probabilitas secara merata.
4.4.3.2 Rekayasa Fitur Geofisika dan Spasial yang Ekstensif
Kapasitas generalisasi sebuah arsitektur deterministik bertumpu kuat pada
```
kualitas komprehensif matriks variabel prediktor (Fitur ). Pemodelan ini𝑋
```
mengabaikan ketergantungan konvensional pada atribut dasar dengan
mengekstraksi dan merakit tujuh parameter fisis secara komposit. Variabel
```
tersebut meliputi metrik geofisika empiris murni berupa magnitudo (magnitude)
```
```
dan batas penetrasi kedalaman hiposentrum (depth), yang kemudian disinergikan
```
dengan fitur hasil rekayasa analitik keruangan meliputi metrik jarak gempa
```
terdekat (nearest_event_km), agregasi kepadatan radius seratus kilometer
```
```
(event_density_100km), nilai kemungkinan inklusi klaster HDBSCAN
```
```
(hdbscan_probability), beserta jangkar koordinat absolut (latitude dan longitude).
```
Korelasi multivariat antar-dimensi inilah yang diproses secara aljabar non-linier
```
untuk meramalkan variabel dependen target ( ) berlabel risk_label. Target𝑦
```
27
ekuilibrium ini memetakan keparahan observasi ke dalam empat strata
```
kedaruratan fisis, Risiko Rendah (LOW), Risiko Sedang (MEDIUM), Risiko Tinggi
```
```
(HIGH), dan Risiko Sangat Tinggi (VERY_HIGH).
```
4.4.3.3 Konstruksi Aliran Pemrosesan dan Persiapan Data
Guna memenuhi prasyarat operasi matriks matematis pada fungsionalitas
algoritma pohon keputusan, himpunan data riwayat kegempaan diwajibkan
melewati pipeline pra-pemrosesan yang disiplin. Dimulai dengan pengambilan
11.403 observasi murni terverifikasi dari pangkalan data PostgreSQL. Penskalaan
fitur secara geometris dieksekusi menggunakan StandardScaler guna
menormalisasi seluruh atribut numerik ke nilai rata-rata absolut nol dan varians
satu. Transformasi ini mencegah fluktuasi galat gradien akibat skala besaran
```
parameter yang terlampau jauh. Target semantik leksikal (risk_label) selanjutnya
```
diproyeksikan ke dalam format integer numerik melalui instrumen LabelEncoder.
Gambar 4.5 Logs Data Splitting
Merespons kenyataan geologis di mana kelas bencana fatal berpopulasi
```
ekstrem minoritas (strata VERY_HIGH menempati proporsi 0,1% dan kelas HIGH
```
```
merajai 80,1% populasi), metodologi pengacakan naif diabaikan dan digantikan
```
mutlak dengan metodologi stratified 80-20 split. Sebagaimana didokumentasikan
dalam log eksekusi fungsional pada gambar di atas, sistem secara kaku
mempartisi dataset menjadi wadah pelatihan berisi 9.122 sampel murni dan
wadah pengujian berisi 2.281 sampel terisolasi. Penjagaan distribusi stratifikasi
```
ini meniadakan risiko buta informasi (information blindness) dari sisi algoritma
```
komputasi tatkala menjumpai karakteristik sinyal bahaya asimetris.
28
4.4.3.4 Struktur Komprehensif Rekayasa Perangkat Lunak Terintegrasi
Penerapan rekayasa fungsional di tataran lingkungan komputasi dikapsulasi
```
melalui paradigma Pemrograman Berorientasi Objek (OOP) ke dalam sebuah
```
kerangka modular kelas berbasis Python ber nomenklatur
EarthquakeClusterClassifier. Modul konstruktor inti merumuskan koneksi aman
terenkripsi menuju database relasional serta menginisialisasi parameter hiper
arsitektural Random Forest dan XGBoost secara simultan. Rutin operasional
fungsional lainnya dienkapsulasi khusus untuk menangani orkestrasi manipulasi
matriks, penskalaan, hingga pendelegasian blok eksekusi perintah pelatihan
komputasi ke dalam lambung pemrosesan machine learning.
Gambar 4.6 Logs Training Classification Models
Proses penyelesaian pelatihan untuk kedua arsitektur model dan perolehan
rasio signifikansi secara deterministik diverifikasi melalui bukti log eksekusi sistem
pada gambar di atas. Melalui rutinitas tersebut, log analitis secara objektif
membedah dan merekam rasio persentase tingkat signifikansi dari tiap-tiap
```
variabel masukan spasial (feature importance). Pada zona muara arsitektur, blok
```
operasional sub-metode dipicu guna mencetak piktogram resolusi tinggi dari
```
matriks kekeliruan probabilitas (confusion matrix), merender agregasi grafik
```
komparasi akurasi, dan menghimpun dokumentasi hasil penaksiran model untuk
diestafetkan pada peladen storage otoritas terkait.
4.4.3.5 Penyatuan Fungsional dengan Orkestrasi Apache Airflow DAG
```
Kesiapan peluncuran instrumen klasifikasi tingkat produksi (production level
```
```
deployment) dijamin mutlak melalui integrasinya sebagai urutan fungsional Task
```
```
kelima dalam penjadwalan Directed Acyclic Graph (DAG) peladen orkestrasi
```
Apache Airflow. Ketentuan desain ini menggariskan regulasi operasional di mana
aktivasi penyulutan algoritma klasifikasi hanya diizinkan berputar setelah siklus
rekayasa keruangan fitur dan klasterisasi kerapatan HDBSCAN divonis paripurna
seratus persen. Untuk menghindari latensi intervensi manual, arsitektur tata
```
laksana memanfaatkan saluran kapiler Cross-Communication (XCom).
```
Fungsionalitas sinyal telemetri XCom beroperasi mandiri mentransmisikan data
```
mentah ekuivalensi matriks performa probabilitas (F1-Score, rasio ketepatan
```
29
```
metrik model pamungkas) lintas simpul eksekusi. Konvergensi logistik tersebut
```
pada akhirnya merajut tautan rekam jejak jalur aset artefak piktogram visual ke
direktori persisten cakram penyimpanan internal, melestarikan integritas validasi
pangkalan penanggulangan risiko bencana secara real-time.
30
BAB 5 HASIL & PEMBAHASAN
5.1 Kinerja Infrastruktur Big Data Pipeline
Implementasi Apache Airflow yang diintegrasikan dengan pustaka Polars
in-memory terbukti secara fungsional mampu menangani alur pemrosesan
bervolume masif. Sistem berhasil menyeleksi 11.403 rekaman unik dan
melakukan standardisasi nilai kosong. Keberhasilan komputasi ini sangat krusial
karena berhasil mentransformasikan data mentah berformat GeoJSON menjadi
pangkalan data relasional terstruktur di dalam PostGIS yang dijamin integritas
spasialnya.
Untuk memverifikasi hasil luaran dari pipeline tersebut, dilakukan inspeksi
langsung terhadap tabel pangkalan data di PostGIS, sebagaimana disajikan pada
Gambar 5.1 berikut.
```
Gambar 5.1 Hasil Spasial Preprocessing Dalam PostGIS (Warehouse)
```
Berdasarkan Gambar 5.1, terlihat dengan jelas bahwa infrastruktur big data
```
pipeline telah berhasil menyatukan atribut historis gempa (seperti magnitude,
```
```
depth, latitude, dan longitude) dengan fitur-fitur baru hasil rekayasa geospasial
```
```
(spatial feature engineering). Beberapa bukti keberhasilan transformasi data yang
```
dapat dianalisis dari tabel tersebut antara lain:
1. Transformasi Geometri: Atribut koordinat mentah telah berhasil dikonversi
dan disimpan ke dalam kolom location bertipe data geometri khusus PostGIS
```
(ditandai dengan format heksadesimal EWKB seperti
```
```
0101000020E6100000...). Format ini memungkinkan pangkalan data untuk
```
melakukan kalkulasi jarak Euclidean dengan sangat cepat.
2. Ekstraksi Fitur Spasial (Spatial Enrichment): Enam fitur baru yang
```
sebelumnya diproses pada tahap enrichment (lihat log eksekusi Airflow pada
```
31
```
Bab 4) telah berhasil dimuat secara sempurna. Fitur-fitur komputasional
```
```
tersebut meliputi jarak ke gempa terdekat (nearest_event_km), kepadatan
```
```
dalam radius 100 km (event_density_100km), hingga penentuan nilai
```
spatial_risk_score.
3. Identifikasi Zona Regional: Atribut seismic_zone berhasil dipetakan secara
otomatis berdasarkan koordinat letak benua. Pada tabel terlihat sistem
mampu mengkategorikan kejadian gempa ke dalam wilayah sabuk tektonik
makro, seperti Japan-Kuril Arc, Himalayas Collision Zone, dan
Indonesia-Philippines Arc.
Pangkalan data yang telah di processed, clean, dan enrich akan parameter
geofisika inilah yang memastikan bahwa algoritma kecerdasan buatan terhindar
dari bias komputasi akibat data yang kotor. Matriks fitur yang terstruktur rapi ini
kemudian diteruskan sebagai fondasi utama untuk eksekusi algoritma HDBSCAN
pada tahap selanjutnya.
Selain keberhasilan transformasi struktur data teknis, pangkalan data PostGIS
```
ini juga memungkinkan sistem untuk mengekstraksi wawasan (insight) geofisika
```
secara langsung melalui query SQL. Sebagai bentuk validasi terhadap kualitas
data mentah yang diserap dari API USGS, dilakukan penyaringan data untuk
```
melihat 5 kejadian gempa bumi dengan pelepasan energi (magnitudo) paling
```
destruktif selama periode pengamatan. Hasil penyaringan tersebut disajikan pada
Tabel 5.1.
Table 5.1 Hasil Spasial Preprocessing Dengan Magnitude Tertinggi
No Place Magnitude
```
1 2025 Mandalay, Burma (Myanmar) Earthquake 7,7
```
2 2025 Aomori Prefecture, Japan Earthquake 7,6
3 129 km ESE of Bitung, Indonesia 7,4
4 12 km E of Santiago, Philippines 7,4
5 98 km ENE of Miyako, Japan 7,4
Berdasarkan Tabel 5.1, data yang dikumpulkan tervalidasi dengan baik dan
sesuai dengan catatan historis kebencanaan nyata. Tercatat gempa bumi di
Mandalay, Myanmar dan Aomori, Jepang menjadi anomali seismik paling
merusak dengan magnitudo menyentuh 7,7 dan 7,6 SR. Kelima wilayah pada
tabel tersebut seluruhnya berlokasi tepat di atas patahan Ring of Fire Asia, yang
semakin menguatkan urgensi penggunaan algoritma klasterisasi spasial pada
tahapan selanjutnya untuk memetakan koridor bahaya dari titik-titik destruktif
tersebut.
Infrastruktur big data pipeline berbasis arsitektur Apache Airflow dan Polars
terbukti sukses menanggulangi kompleksitas pemrosesan data seismik
bervolume masif. Eksekusi penarikan data dari API USGS secara otonom berhasil
menyaring, membersihkan, dan mendeduplikasi metadata hingga menghasilkan
dataset akhir berkualitas tinggi.
32
Gambar 5.2 Diagram Earthquake Distribution by Year
Berdasarkan grafik distribusi di atas, pipeline telah berhasil mengamankan
total 11.403 rekaman observasi murni yang tersebar secara proporsional
melintasi tahun pelaporan 2024 hingga 2026. Konsistensi tingkat kepadatan
pengumpulan data ini membuktikan bahwa arsitektur komputasional terbebas
```
dari defisit temporal laten maupun bias kesenjangan pelaporan (jangka waktu).
```
Ketiadaan bias temporal ini menjamin bahwa model klasifikasi tidak akan
mengalami overtraining pada pola aktivitas seismik musiman tertentu. Seluruh
data tervalidasi ini kemudian sukses ditransformasikan ke dalam format geometry
khusus pada pangkalan data spasial PostGIS, diperkaya dengan enam fitur
```
rekayasa keruangan (termasuk nearest_event_km dan event_density_100km),
```
dan siap diinjeksikan ke dalam ekosistem machine learning.
```
5.2 Pola Distribusi Spasial (HDBSCAN)
```
Dari total 11.403 observasi, komputasi HDBSCAN berhasil membentuk 194
klaster patahan seismik dan secara agresif memisahkan 5.917 titik kejadian
sebagai noise. Tingginya rasio titik noise membuktikan bahwa HDBSCAN memiliki
sensitivitas yang sangat ketat untuk memastikan bahwa hanya episentrum yang
benar-benar berulang di jalur patahan yang dikelompokkan menjadi klaster.
Untuk memberikan interpretasi geografis terhadap hasil komputasi ini, dilakukan
```
pemetaan visual (visualisasi spasial) di atas basemap kawasan Asia.
```
33
```
Gambar 5.3 Visualisasi Cluster Gempa HDBSCAN Dengan Titik Noise (Anomali)
```
Pada Gambar 5.2, titik-titik abu-abu transparan merepresentasikan data
```
noise (sebanyak 5.917 titik atau sekitar 51,89% dari total earthquake yang
```
```
terkumpulkan). Secara geofisika, sistem secara cerdas berhasil mengabaikan
```
```
kejadian gempa latar (background seismicity) yang sifatnya sporadis.
```
```
Pembersihan data ini secara dramatis meningkatkan rasio Signal-to-Noise (SNR),
```
memastikan bahwa fitur masukan yang dikirim ke tahap klasifikasi terhindar dari
false-positive pada area bergetaran rendah.
34
```
Gambar 5.4 Visualisasi Cluster Gempa HDBSCAN Tanpa Titik Noise (Anomali)
```
```
Gambar 5.3 menyajikan peta kawasan spasial murni tanpa noise (sebanyak
```
```
5.485 titik atau sekitar 48,11% dari total earthquake yang terkumpul). Melalui
```
visualisasi ini, terlihat jelas keunggulan HDBSCAN dibanding algoritma K-Means
```
(yang cenderung memaksakan klaster membulat). Warna-warna klaster yang
```
dibentuk oleh HDBSCAN tersusun rapi membentang memanjang dan melengkung
di sepanjang batas zona subduksi raksasa Asia, mengikuti anatomi sesar secara
natural. Klaster-klaster padat terpetakan menyusuri rute Ring of Fire, mulai dari
parit subduksi Sunda Megathrust di selatan Indonesia, melingkari kepulauan
Filipina, hingga membentang di pesisir Timur Jepang dan sabuk tumbukan
Pegunungan Himalaya.
Hasil pelabelan klaster ini juga mengelompokkan 71% data ke dalam risiko
"HIGH" dan 9,4% pada risiko "VERY HIGH". Tingginya densitas klaster gempa
berbahaya yang saling terhubung antar negara ini memvalidasi hipotesis bahwa
mitigasi bencana seismik tidak bisa lagi dibatasi oleh sekat administratif satu
negara saja, melainkan harus menggunakan pendekatan regional map hazard.
Hasil pelabelan klaster dan kepadatan titik kejadian gempa ini kemudian
```
dipetakan ke dalam label tingkat risiko bahaya (Risk Label Distribution). Hasil
```
agregasi pelabelan dari keseluruhan dataset mencatat persentase sebaran
sebagaimana disajikan pada Tabel 5.1 berikut:
Table 5.2 Filtering Risk Label Berdasarkan Total Earthquake Recollection
```
Risk Label Records (Earthquake) Differences (Percent)
```
Very High 16 0,1%
High 9.138 80,1%
35
```
Risk Label Records (Earthquake) Differences (Percent)
```
Medium 2.014 17,7%
Low 235 2,1%
Berdasarkan Tabel 5.2, tergambar jelas realitas geofisika benua Asia yang
```
sangat rawan. Fakta bahwa mayoritas absolut data (71%) terklasifikasi sebagai
```
risiko tingkat "HIGH" dan 9,4% masuk dalam kategori "VERY HIGH" memvalidasi
status kawasan Asia sebagai area Ring of Fire yang sangat aktif dan merusak.
Di sisi lain, dari scope data science, Tabel 5.1 menunjukkan bahwa dataset
```
kebencanaan ini memiliki rasio ketimpangan kelas yang sangat tinggi (highly
```
```
imbalanced dataset). Kelas minoritas (Low dan Very High) jauh lebih sedikit
```
```
dibandingkan kelas mayoritas (High). Data klaster beserta pelabelan risiko inilah
```
yang kemudian dimuat ke dalam matriks fitur dan dilanjutkan sebagai data target
```
latih (training target) untuk diuji performanya menggunakan algoritma klasifikasi
```
XGBoost dan Random Forest pada tahap berikutnya.
5.3 Evaluasi Kinerja Algoritma Klasifikasi
Tujuan fundamental pelibatan arsitektur machine learning adalah untuk
menerjemahkan identitas klaster geospasial menjadi prediksi probabilitas
keparahan ancaman yang presisi. Evaluasi komparatif antara Random Forest dan
```
XGBoost dilakukan dengan mengaudit Matriks Kekeliruan (Confusion Matrix) dan
```
metrik fungsionalnya untuk memastikan ketepatan penanganan kelas minoritas
yang krusial.
```
5.3.1 Signifikansi Inklusi Titik Anomali (Noise Inclusion) dalam
```
Formulasi Klasifikasi
Keputusan metodologis paling mendobrak dalam riset ini adalah
```
mempertahankan titik pencilan (noise dengan cluster_id = -1) untuk dilibatkan
```
dalam pembelajaran model terawasi. Secara ortodoks, titik derau seringkali
dieliminasi untuk meringankan beban komputasi. Namun, analisis data
membuktikan bahwa 100% observasi kejadian kiamat peradaban tingkat
```
VERY_HIGH berlokasi di area patahan yang terisolasi (intraplate anomaly)
```
sehingga jatuh pada taksonomi titik buangan HDBSCAN. Menghapus titik-titik
noise tersebut akan mengakibatkan sistem klasifikasi buta sama sekali terhadap
eksistensi parameter fisis dari gempa paling destruktif.
```
5.3.2 Analisis Komparatif Dimensi Performa (Random Forest vs
```
```
XGBoost)
```
Komparasi sirkuit komputasi Random Forest dan XGBoost divonis melalui
```
evaluasi pada wadah partisi set pengujian (test set) yang sepenuhnya
```
independen.
36
Gambar 5.5 Logs Tahap Evaluasi
Data log komputasi Airflow secara definitif merangkum pencapaian model
yang kemudian diuraikan dalam matriks Tabel 5.3 di bawah ini:
Table 5.3 Rekapitulasi Komparatif Evaluasi Metrik Prediksi Arsitektur Algoritma
Parameter Metrik
Evaluasi
Metrik Random
Forest
Metrik XGBoost Selisih Diferensiasi
Kinerja Mutlak
```
Akurasi (Accuracy) 0.9860 (98,60%) 0.9921 (99.21%) + 0.0061 (+ 0,61%)
```
```
Presisi (Precision) 0.9865 (98,65%) 0.9921 (99,21%) + 0.0056 (+ 0,56%)
```
```
Sensitivitas (Recall) 0.9860 (98,60%) 0.9921 (99,21%) + 0.0061 (+ 0,61%)
```
```
Skor Harmonik (F1-Score) 0.9861 (98,61%) 0.9920 (99,20%) + 0.0059 (+ 0,59%)
```
Analisis empiris menunjukkan bahwa kedua model sukses mendobrak batas
```
akurasi 98%, memvalidasi efikasi gabungan fitur keruangan (HDBSCAN) dan
```
parameter geofisika. Akan tetapi, XGBoost menunjukkan taring supremasinya
dengan pencapaian akurasi absolut 99,21%, menyapu bersih performa Random
Forest di seluruh metrik pengujian. Keunggulan ini dipicu secara langsung oleh
metode optimasi gradient descent XGBoost yang mampu menyeimbangkan
toleransi penalti galat terhadap kelas data seismik Asia yang asimetris, mencegah
kegagalan identifikasi pada kejadian minoritas.
Gambar 5.6 Hasil Evaluasi Klasifikasi
37
```
5.3.3 Dekonstruksi Matriks Kekeliruan (Confusion Matrix)
```
Agregasi akurasi global yang tinggi acapkali mengaburkan bias performa
mesin dalam mengidentifikasi probabilitas kategori minoritas. Oleh karena itu,
bedah Confusion Matrix diwajibkan untuk menjamin tidak adanya anomali
tebakan pada level insiden krusial.
Gambar 5.7 Heatmap Random Forest Confusion Matrix
Model dasar Random Forest membuktikan keandalannya dalam mengunci
```
prediksi kelas mayoritas (HIGH). Namun, arsitektur bagging paralel ini
```
menunjukkan kelemahan presisi di sekitar garis demarkasi batas wilayah
```
menengah (abu-abu). Ia melemparkan tuduhan predikat kelas MEDIUM ke 20
```
```
observasi yang sesungguhnya berbahaya (HIGH), dan sebaliknya mendeteksi 10
```
observasi menengah sebagai gempa mematikan. Kegoyahan di ambang batas ini
secara logis merumuskan "Alarm Palsu" yang memicu inefisiensi mitigasi. Pada
kelas ekstrem, RF hanya mampu menangkap 3 observasi VERY_HIGH secara
akurat akibat keterbatasannya mencerna data minoritas.
38
Gambar 5.8 Heatmap XGBoost Confusion Matrix
XGBoost mengoreksi drastis ketidakseimbangan tebakan tersebut.
Kemampuan optimasi fungsi regulerisasinya mengembalikan maruah presisi pada
perbatasan, merekam 1.822 tangkapan akurat untuk kelas HIGH. Hal yang paling
fenomenal adalah performa sensitivitasnya pada observasi langka. Meskipun
distribusi kelas VERY_HIGH dan LOW sangatlah kecil, mesin ini mencatatkan recall
```
perlindungan nyawa (life-safety recall) dengan tangkapan minoritas yang nyaris
```
paripurna, meminimalisasi potensi distorsi False Negatives yang merupakan
esensi dari arsitektur penyelamatan jiwa.
5.4 Implikasi Hasil Penelitian Terhadap Mitigasi Bencana
39
BAB 6 PENUTUP
40
DAFTAR REFERENSI
WIJAYA, Ody Octora, et al. Analysis of Sulawesi Earthquake Data from 2019 to
```
2023 using DBSCAN Clustering. Jurnal RESTI (Rekayasa Sistem dan Teknologi
```
```
Informasi), 2024, 8.4: 454-465.
```
NATAWIDJAJA, Danny Hilman, et al. The 2018 M w7. 5 Palu ‘supershear’
earthquake ruptures geological fault's multisegment separated by large
```
bends: results from integrating field measurements, LiDAR, swath
```
bathymetry and seismic-reflection data. Geophysical Journal International,
2021, 224.2: 985-1002.
PUSPITA, D. D., et al. Random Forest Analysis for Predicting the Probability of
Earthquake in Indonesia. SOCIAL SCIENCE AND HUMANITIES JOURNAL
Учредители: Everant Publisher, 2025, 9.01: 6295-6304.
SALEEM, Muhammad Asim, et al. Neural-XGBoost: A hybrid approach for disaster
prediction and management using machine learning. IEEE Access, 2025.
```
PANDA, Anurag; YADAV, Gaurav Kumar. Earthquake Damage Grades Prediction
```
using An Ensemble Approach Integrating Advanced Machine and Deep
Learning Models. arXiv preprint arXiv:2506.22129, 2025.
HARIG, Sven, et al. The Tsunami Scenario Database of the Indonesia Tsunami
```
Early Warning System (InaTEWS): evolution of the coverage and the
```
involved modeling approaches. Pure and Applied Geophysics, 2020, 177:
1379-1401.
```
Debnath, M., Tripathi, P. K., & Elmasri, R. (2015, September). K-DBSCAN:
```
Identifying spatial clusters with differing density levels. In 2015
```
International workshop on data mining with industrial applications (DMIA)
```
```
(pp. 51-60). IEEE.
```
HINGA, Bethany D. Rinard. Ring of fire: an encyclopedia of the Pacific Rim's
earthquakes, tsunamis, and volcanoes. Bloomsbury Publishing USA, 2015.
ESTER, Martin, et al. A density-based algorithm for discovering clusters in large
spatial databases with noise. In: kdd. 1996. p. 226-231.
MOUSAVI, S. Mostafa, et al. Earthquake transformer—an attentive deep-learning
model for simultaneous earthquake detection and phase picking. Nature
communications, 2020, 11.1: 3952.
```
MCINNES, Leland; HEALY, John. Accelerated hierarchical density based clustering.
```
```
In: 2017 IEEE international conference on data mining workshops (ICDMW).
```
IEEE, 2017. p. 33-42.
NAOI, Makoto, et al. Neural phase picker trained on the Japan meteorological
agency unified earthquake catalog. Earth, Planets and Space, 2024, 76.1:
150.
41
MORANTE-CARBALLO, Fernando, et al. Systematic Review on Seismic Hazards in
the Coastal Regions of the Pacific Ring of Fire. International Journal of
Safety & Security Engineering, 2024, 14.5.
KE, Siao-Syun, et al. Leveraging Big Data for Earthquake Disaster Response:
Insights from Taiwanese Decision-Support Systems from 1999 to 2024.
Bulletin of the Seismological Society of America, 2025.
Yavas CE, Chen L, Kadlec C, Ji Y. Improving earthquake prediction accuracy in Los
```
Angeles with machine learning. Sci Rep. 2024 Oct 18;14(1):24440. doi:
```
10.1038/s41598-024-76483-x. Retraction in: Sci Rep. 2026 Apr
```
20;16(1):12864. doi: 10.1038/s41598-026-49689-4. PMID: 39424892;
```
```
PMCID: PMC11489593.
```
42