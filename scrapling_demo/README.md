# Scrapling Demo

README này tập trung vào:

- `scrapling_demo/crawl4ai_benhvien_scraper.py` (crawler demo chính)
- `craw_thuocbietduoc.py` (crawler thuocbietduoc.com.vn, ghi DB `*_bietduoc`)
- `craw_boyte.py` (crawler `moh.gov.vn`, ghi DB `*_boyte`)

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

## Crawl thuocbietduoc.com.vn (`craw_thuocbietduoc.py`)

Script này crawl nội dung HTML của `thuocbietduoc.com.vn`, trích nội dung trong `<main>`, tìm media/file và ghi vào các bảng:

- `data_bietduoc`
- `image_bietduoc`
- `video_bietduoc`
- `file_bietduoc`

### Chạy cơ bản

```bat
python craw_thuocbietduoc.py
```

### Resume + retry lỗi trước đó

```bat
python craw_thuocbietduoc.py --retry-failed-on-resume
```

### Giảm lỗi Playwright kiểu “object has been collected…”

Nếu thấy log lỗi như:

- `BrowserContext.new_page: The object has been collected to prevent unbounded heap growth`

hãy giảm tải Playwright bằng cách giới hạn browser concurrency và bật recycle:

```bat
python craw_thuocbietduoc.py --concurrency 6 --browser-concurrency 1 --recycle-every 200
```

### Các option chính (thuocbietduoc)

- `--start-url <url>`
- `--max-pages <n>`: `0` là không giới hạn
- `--concurrency <n>`: số worker xử lý queue URL
- `--browser-concurrency <n>`: giới hạn số tác vụ Playwright chạy song song (khuyến nghị 1–2)
- `--recycle-every <n>`: recycle crawler/browser mỗi N lần crawl (0 = tắt)
- `--connect-error-retry-limit <n>`: số lần retry cho lỗi mạng / lỗi Playwright “tạm thời”
- `--page-timeout-ms <ms>`
- `--output-root <dir>`
- `--no-resume`
- `--retry-failed-on-resume`
- `--allow-domain <domain>`: lặp lại được

### Tùy chỉnh nhanh bằng biến cấu hình (không cần nhớ option)

Ở đầu file `craw_thuocbietduoc.py` có các biến `DEFAULT_*` (ví dụ: `DEFAULT_CONCURRENCY`, `DEFAULT_BROWSER_CONCURRENCY`, `DEFAULT_RECYCLE_EVERY`, ...) để bạn chỉnh 1 chỗ rồi chạy bình thường.

## Crawl moh.gov.vn (`craw_boyte.py`)

Script này crawl nội dung HTML của `moh.gov.vn`, trích nội dung chính theo các selector đã cấu hình, tìm media/file và ghi vào các bảng:

- `data_boyte`
- `image_boyte`
- `video_boyte`
- `file_boyte`

### Chạy cơ bản

```bat
python craw_boyte.py
```

### Resume mặc định

Mặc định script có resume. Khi chạy lại, nó đọc file state cũ và:

- bỏ qua URL đã `done ok`
- chạy lại URL đã `start` nhưng chưa `done` (ví dụ stop giữa chừng)
- bỏ qua URL đã `done failed`

Nếu muốn tắt resume:

```bat
python craw_boyte.py --no-resume
```

### Retry URL fail khi resume

Mặc định các URL đã fail trước đó sẽ không chạy lại khi resume.

Nếu muốn chạy lại cả các URL fail:

```bat
python craw_boyte.py --retry-failed-on-resume
```

### Retry trong cùng một lần chạy

Script có retry trong cùng một lần chạy, nhưng chỉ áp dụng cho nhóm lỗi mạng/kết nối tạm thời.

Ví dụ các lỗi có thể được retry:

- connection reset
- connection refused
- network changed
- timeout kiểu kết nối

Các lỗi như:

- `wait condition failed`
- `waiting for selector`

hiện không được coi là lỗi mạng, nên sẽ không retry trong cùng run.

### Các option chính (boyte)

- `--start-url <url>`
- `--max-pages <n>`: `0` là không giới hạn
- `--concurrency <n>`
- `--page-timeout-ms <ms>`
- `--output-root <dir>`
- `--no-resume`
- `--retry-failed-on-resume`
- `--allow-domain <domain>`: lặp lại được

### Media/file lưu DB như thế nào

Nếu một page crawl thành công và tạo được `PageItem`, script sẽ:

- ghi nội dung trang vào `data_boyte`
- ghi ảnh vào `image_boyte`
- ghi video vào `video_boyte`
- ghi file vào `file_boyte`

Nếu page bị `crawl_failed`, `fetch_error`, hoặc không lấy được `main content`, thì page đó sẽ không ghi media/file vào DB.

### Ghi chú về ảnh lấy từ markdown

Parser media đã được chỉnh để xử lý đúng URL ảnh markdown có ký tự escape như:

- `\(`
- `\)`

Ví dụ URL kiểu:

- `https://moh.gov.vn/.../Frame%20427319943%20\(1\).png`

sẽ không còn bị cắt dở giữa chừng khi parse từ markdown.

## File state

Crawler lưu state để resume trong thư mục `data_craw/state`:

- `events_db.jsonl`
- `events_pdf_db.jsonl`
- `events_raw_pdf_db.jsonl`
- `events_boyte_db_<domain>_<hash>.jsonl`

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
- `events_boyte_db_<domain>_<hash>.jsonl`: trạng thái crawl của `craw_boyte.py`

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
