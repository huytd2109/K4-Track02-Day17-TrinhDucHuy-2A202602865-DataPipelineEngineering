# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trịnh Đức Huy/2A202602865
**Repo:** https://github.com/huytd2109/K4-Track02-Day17-TrinhDucHuy-2A202602865-DataPipelineEngineering
**Commit bài nộp:** 6d9b494355715b6c927710a0c5bbee6178e4e163
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Codex hỗ trợ viết code
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Hai tests checkpoint fail: 24 hàng / 12 ticket; T-91 có ba trạng thái thay vì chỉ `high / closed / bug`. | Ba tests checkpoint fail: feature lệch full recompute; u05 ngày 12/08 chỉ có `(2, 1, 0)` thay vì `(5, 3, 1)`. | Hai tests delete fail: T-97 vẫn có `is_deleted = false`, còn nội dung và có một hàng trong snapshot mới nhất. |
| **Nguyên nhân gốc** | `row_number()` theo `_lsn DESC` chỉ dedup trong batch; `INSERT` nối thêm hàng giữa các batch, không cập nhật theo `ticket_id`. | `LOOKBACK_DAYS = 0` chỉ tính lại ngày ingest hiện tại; sự kiện ngày 12 đến ngày 15 không cập nhật partition ngày 12. | Parser lấy khoá chỉ từ `after`; delete có `after = null` nên bị lọc bởi `ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: dùng `MERGE` theo `ticket_id`; insert khoá mới, update toàn bộ trạng thái khi `s._lsn > t._lsn`. LSN bằng hoặc cũ hơn không ghi đè. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = 3`. `pipeline/gold.py` đã lọc và nhóm theo `CAST(event_time AS DATE)`, không cần sửa. | `pipeline/staging.py`: `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`; chỉ fallback khoá, nội dung vẫn lấy từ `after`; giữ `_op`, `_lsn`. |
| **Khái niệm trên slide** | CDC log ordering (LSN), keyed upsert và idempotency; phân biệt dedup trong batch với ghi trạng thái hiện tại giữa các batch. | Event time khác ingest time; late data cần cửa sổ lookback và overwrite-partition theo event date. | CDC delete là thay đổi có LSN; Kafka tombstone `value = null` phục vụ compaction, giữ ở Bronze nhưng không áp dụng như một thay đổi. |

## 2. Các con số

- P99 lateness đo từ 43 bản ghi Bronze: `3.00` ngày (P95 `2.90`, max `3`) → `LOOKBACK_DAYS = 3`; lấy `ceil(P99)` để không làm tròn xuống khi percentile có phần lẻ. Cửa sổ bao gồm `[day - 3, day]`, nên batch ngày 15 tính lại ngày 12.
- `submission/checksums.txt`: **PASS** — `C0 = C1 = C2 = C3 = 39e115c510ecdf526800eac227158a4f` (Gold combined).
- Parity: **PARITY** — cả `silver_tickets` (`3c15dfd43701`) và `gold_feature_daily` (`8630e04a61d1`) có checksum hiển thị giống nhau giữa Python và dbt.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giữ trạng thái mới nhất theo LSN; xoá rồi tính lại partition trong lookback từ Silver giúp cập nhật sự kiện muộn mà không cộng trùng.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khoá và LSN delete để replay LSN cũ không hồi sinh ticket, đồng thời đặt nội dung thành null.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: giữ khả năng tái lập tập huấn luyện theo dữ liệu đã biết, snapshot sau delete loại T-97.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: seed nhỏ chạy cục bộ; dbt bổ sung dependency, contracts, data/unit tests và microbatch mà không cần cụm xử lý.

dbt dùng profile `day17/dev`, DuckDB `dbt.duckdb`, một thread; staging là view,
Silver/Gold là incremental. `unique_key='ticket_id'` xác định khoá MERGE;
`merge_update_condition` chỉ update khi LSN nguồn lớn hơn đích.
`batch_size='day'` chia Gold theo ngày event time; `lookback=3` cho phép tính lại
ba batch ngày trước khi chạy incremental để nhận late data. Khoảng build
`[2026-08-10, 2026-08-17)` gồm đủ bảy ngày đến 16/08.
Parity chỉ đối chiếu hai bảng chung, không chứng minh dbt có SCD2, transcripts,
quarantine, training snapshots hay doc chunks như Python.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   Trong lab giữ snapshot cũ để tái lập, loại T-97 khỏi snapshot mới nhất và RAG.
   Production cần ưu tiên chính sách xoá: thu hồi snapshot chứa dữ liệu bị xoá,
   tạo phiên bản đã loại dữ liệu và ghi dấu kiểm toán không chứa PII; kiểm tra
   cả Bronze, transcript, cache embedding, bản export và backup. Các tests lab
   không chứng minh xoá PII đầy đủ; regex vẫn có thể để lại tên người.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

Thử thách 1 — kiểm chứng ngày 2026-10-04 trên lake/warehouse tạm của tests:
hai tests checkpoint pass, Silver có 12 hàng / 12 ticket và T-91 chỉ còn
`high / closed / bug`. Chỉ sửa `pipeline/silver.py` và REPORT; dùng tests có sẵn.
Chưa kiểm chứng toàn bộ pipeline,
late data, delete hay dbt ở bước này. Warehouse có sẵn trong repo chưa được dựng lại.

```text
PS> .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "one_row_per_ticket or latest_state_wins"
..                                                                       [100%]
2 passed, 11 deselected in 1.98s

PS> .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "silver or scd2"
....                                                                     [100%]
4 passed, 9 deselected in 2.08s
```

Thử thách 2 — kiểm chứng ngày 2026-10-04: ba tests có sẵn pass trên
lake/warehouse tạm, u05 ngày `2026-08-12` có **5 events, 3 clicks, 1 feedback down**;
checksum feature khớp full recompute từ Silver. `event_time` xác định ngày tính
feature; `_ingested_at` xác định thời điểm dữ liệu đến để đo lateness. Sự kiện
ngày 12 đến ngày 15 vẫn được tính vào ngày 12. Chỉ sửa `pipeline/config.py`
và REPORT trong thử thách này; không thêm file hoặc sửa tests.

```text
PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "feature_daily or late_events or lookback"
...                                                                      [100%]
3 passed, 10 deselected in 2.33s

PS> .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "one_row_per_ticket or latest_state_wins"
..                                                                       [100%]
2 passed, 11 deselected in 1.94s
```

Thử thách 3 — kiểm chứng ngày 2026-10-04: verify dựng lại warehouse hiện có,
34 tests có sẵn pass, rerun ngày 12 ba lần giữ nguyên Gold. Sau replay, T-97 có
`is_deleted = true`, `user_id/subject/body = null`, `_lsn = 24020000`,
`_batch_id = 2026-08-15`; không có hàng trong snapshot `v2026-08-16` hoặc RAG
chunks. Snapshot `v2026-08-12`..`v2026-08-14` vẫn có T-97 theo chủ đích lab.
Chỉ sửa `pipeline/staging.py` và REPORT trong thử thách này;
`submission/checksums.txt` do lệnh rerun sinh tự động, không thêm file test.

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.53s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

Thử thách 4 — kiểm chứng ngày 2026-10-04: dbt-core `1.12.5`, dbt-duckdb
`1.11.0`; build bảy batch ngày thành công, `PASS=19`, parity cả hai bảng.
Không sửa model, cấu hình dbt, tests hoặc checker; chỉ cập nhật REPORT.
Output dbt dưới đây giữ nguyên văn bản và thời gian log, bỏ mã màu ANSI và khoảng trắng cuối dòng.

```powershell
$env:PYTHONIOENCODING = 'utf-8'
.\.venv\Scripts\python.exe main.py --land-only
$env:DO_NOT_TRACK = '1'
Push-Location dbt_project
try {
    ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
} finally {
    Pop-Location
}
.\.venv\Scripts\python.exe -m scripts.parity
```

```text
PS> .\.venv\Scripts\python.exe main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)

PS (dbt_project)> ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:50:57  Running with dbt=1.12.5
03:50:58  Registered adapter: duckdb=1.11.0
03:50:59  Unable to do partial parsing because saved manifest not found. Starting full parse.
03:51:04  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:51:04
03:51:04  Concurrency: 1 threads (target='dev')
03:51:04
03:51:08  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:51:09  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.14s]
03:51:09  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:51:09  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.05s]
03:51:09  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:51:09  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.35s]
03:51:09  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:51:09  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.25s]
03:51:09  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:51:09  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.15s]
03:51:09  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:51:09  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.07s]
03:51:09  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:51:09  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
03:51:09  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:51:10  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
03:51:10  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:51:10  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
03:51:10  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:51:10  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
03:51:10  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:51:10  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
03:51:10  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:51:10  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
03:51:10  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:51:10  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
03:51:10  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:51:10  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
03:51:10  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:51:10  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
03:51:10  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:51:10  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
03:51:10  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.09s]
03:51:10  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.04s]
03:51:10  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
03:51:10  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.05s]
03:51:10  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
03:51:10  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:51:10  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.05s]
03:51:10  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.42s]
03:51:10  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:51:10  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
03:51:10  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:51:10  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
03:51:10  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:51:10  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.04s]
03:51:10
03:51:10  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 6.29 seconds (6.29s).
03:51:10
03:51:10  Completed successfully
03:51:10
03:51:10  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
