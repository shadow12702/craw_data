# Scrapling Demo

README này tập trung vào file `scrapling_demo/crawl4ai_benhvien_scraper.py`.

## Yêu cầu

- Python 3.10+
- Cài dependency:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Nếu thiếu package cho crawler/PDF/Postgres, cài thêm:

```bat
pip install crawl4ai psycopg[binary] pypdf
```

## Cấu hình DB

File config: `scrapling_demo/db_config.py`

Thông tin hiện tại:

- host: `192.168.200.66`
- port: `5432`
- database: `n8n`
- username: `n8n`
- schema: `n8n`

Các bảng đang dùng:

- `storage_data`
- `storage_image`
- `storage_video`
- `pdf_documents`

Bảng PDF đang map theo:

- table: `TABLE_PDF = "pdf_documents"`
- columns: `PDF_COLUMNS = ("id", "file_name", "file_url", "content", "created_at")`

Lưu ý:

- Script không tự tạo/xóa bảng khi chạy crawler.
- Nếu cần chạy thao tác có tác động DB, phải confirm trước.

## Chạy crawler

### 1. Chạy đầy đủ nội dung site

Mode này crawl nội dung trang HTML bình thường. Nếu bật thêm `--crawl-pdf` thì sẽ xử lý thêm PDF.

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode all
```

Ví dụ giới hạn số trang:

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode all --max-pages 50
```

### 2. Chỉ lấy URL PDF

Mode này vẫn duyệt site để tìm link `.pdf`, nhưng không ghi thêm nội dung HTML/media khác vào DB.

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode pdf-only
```

Kết quả sẽ lưu URL PDF vào bảng PDF theo config, hiện là `pdf_documents`.

### 3. Crawl PDF được phát hiện từ site

Mode `all` + bật crawl PDF:

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode all --crawl-pdf
```

Hiện tại flow này:

- vẫn crawl HTML
- vẫn ghi `storage_data`, `storage_image`, `storage_video`
- đồng thời xử lý thêm các link PDF tìm thấy

### 4. Crawl trực tiếp một URL PDF

Option này nhận trực tiếp URL PDF, không cần đợi discover từ HTML.

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode all --pdf-url "https://example.com/file.pdf"
```

Có thể lặp lại nhiều lần:

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode all --pdf-url "https://example.com/a.pdf" --pdf-url "https://example.com/b.pdf"
```

### 5. Chỉ lưu raw URL PDF

Không tải file, không extract text PDF, chỉ lưu URL vào bảng PDF.

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --raw-pdf-url "https://example.com/file.pdf"
```

Nếu chạy cùng `--crawl-mode pdf-only`, scraper sẽ chỉ tập trung vào luồng PDF.

## Các option chính

- `--crawl-mode all|pdf-only`
- `--start-url <url>`
- `--max-pages <n>`: `0` là không giới hạn
- `--concurrency <n>`
- `--pdf-concurrency <n>`
- `--crawl-pdf`
- `--pdf-url <url>`: lặp lại được
- `--raw-pdf-url <url>`: lặp lại được
- `--allow-domain <domain>`: lặp lại được
- `--page-timeout-ms <ms>`
- `--output-root <dir>`
- `--no-resume`
- `--retry-failed-on-resume`

## File state

Crawler lưu state để resume trong thư mục `data_craw/state`:

- `events_db.jsonl`
- `events_pdf_db.jsonl`
- `events_raw_pdf_db.jsonl`

## Cách đọc log

Khi chạy crawler, terminal sẽ in các nhãn log sau:

- `[HTML] Crawling: ...`: đang duyệt một trang HTML
- `[HTML] Skip DB write in pdf-only mode: ...`: đã duyệt trang HTML nhưng bỏ qua ghi `storage_*` vì đang ở mode `pdf-only`
- `[PDF-DISCOVER] Found ...`: tìm thấy link PDF trong một trang
- `[PDF-DISCOVER] Queue raw PDF URL from page: ...`: đã đưa URL PDF vào hàng đợi raw PDF
- `[PDF-RAW] Saving URL: ...`: đang chuẩn bị lưu URL PDF vào bảng PDF
- `[PDF-RAW] Saved URL to PDF table: ...`: đã lưu URL PDF vào DB thành công
- `[PDF-EXTRACT] Downloading: ...`: đang tải PDF để trích text
- `[PDF-EXTRACT] Saved extracted PDF content to storage_data: ...`: đã lưu nội dung PDF extract vào `storage_data`

### Với `--crawl-mode pdf-only`

Nếu chạy:

```bat
python scrapling_demo\crawl4ai_benhvien_scraper.py --crawl-mode pdf-only
```

Thì flow đúng thường sẽ là:

```text
[HTML] Crawling: ...
[PDF-DISCOVER] Found ...
[PDF-DISCOVER] Queue raw PDF URL from page: ...
[PDF-RAW] Saving URL: ...
[PDF-RAW] Saved URL to PDF table: ...
```

Nếu chỉ thấy:

```text
[PDF-DISCOVER] Found ...
[PDF-DISCOVER] Queue raw PDF URL from page: ...
```

nhưng không thấy:

```text
[PDF-RAW] Saving URL: ...
[PDF-RAW] Saved URL to PDF table: ...
```

thì PDF mới chỉ được phát hiện hoặc đưa vào queue, chưa có bằng chứng là đã lưu xuống DB.

### Kiểm tra file state

- `events_db.jsonl`: trạng thái crawl HTML
- `events_pdf_db.jsonl`: trạng thái PDF extract
- `events_raw_pdf_db.jsonl`: trạng thái raw PDF URL

Nếu chạy `pdf-only`, file quan trọng nhất để check là `events_raw_pdf_db.jsonl`.

### Lỗi đã gặp trước đó

Trước đây `raw_pdf_worker` và `pdf_worker` có thể thoát quá sớm nếu queue PDF trống trong vài giây đầu, trước khi HTML crawler kịp phát hiện PDF.

Hiện tại lỗi này đã được sửa:

- worker PDF chỉ thoát sau khi HTML crawl hoàn tất
- và queue PDF thực sự trống

Nếu vẫn thấy PDF được `Queue` mà không có `Saving URL`, hãy chạy lại bằng code mới nhất rồi kiểm tra tiếp terminal và `events_raw_pdf_db.jsonl`.

## Kiểm tra kết nối DB

Có thể dùng:

```bat
python scrapling_demo\test.py
```

Script này chỉ:

- đọc config DB
- kiểm tra kết nối
- kiểm tra bảng có tồn tại hay không

Script này không tự tạo bảng và không tự ghi/xóa dữ liệu.
