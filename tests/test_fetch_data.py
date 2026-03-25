"""
pytest unit tests for fetch_data.py
Covers: error classes, path safety, HTML parsing, database operations, INDICATORS config.
"""

import sqlite3, os, sys, pathlib, warnings, re, json, tempfile
from unittest import mock

import pytest

# Ensure scripts/ is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import fetch_data as fd


# ─────────────────────────────────────────────────────────────────
# 1. Error Classes
# ─────────────────────────────────────────────────────────────────

class TestErrorClasses:
    def test_fetch_error_default(self):
        e = fd.FetchError("boom")
        assert str(e) == "boom"
        assert isinstance(e, Exception)

    def test_fetch_error_inheritance(self):
        assert issubclass(fd.FetchError, Exception)

    def test_rate_limit_error_attributes(self):
        e = fd.RateLimitError(retry_after=120)
        assert e.retry_after == 120
        assert "120" in str(e)

    def test_rate_limit_error_default(self):
        e = fd.RateLimitError()
        assert e.retry_after == 60

    def test_database_error_default(self):
        e = fd.DatabaseError("bad query")
        assert str(e) == "bad query"
        assert isinstance(e, Exception)


# ─────────────────────────────────────────────────────────────────
# 2. Path Safety Check
# ─────────────────────────────────────────────────────────────────

class TestPathSafety:
    def test_safe_path_allowed(self):
        # Should NOT raise
        fd.check_path_safe("/tmp/my.db", "db-path")
        fd.check_path_safe(str(pathlib.Path(__file__).parent / "data.db"), "db-path")

    def test_protected_windows_path(self):
        with pytest.raises(PermissionError) as exc:
            fd.check_path_safe("C:\\Windows\\System32\\foo.db", "db-path")
        assert "System path detected" in str(exc.value)

    def test_protected_windows_prog_files(self):
        with pytest.raises(PermissionError) as exc:
            fd.check_path_safe("C:\\Program Files\\app\\db", "db-path")
        assert "system path detected" in str(exc.value).lower()

    def test_protected_unix_etc(self):
        with pytest.raises(PermissionError) as exc:
            fd.check_path_safe("/etc/passwd.db", "db-path")
        assert "system path detected" in str(exc.value).lower()

    def test_protected_unix_usr(self):
        with pytest.raises(PermissionError) as exc:
            fd.check_path_safe("/usr/local/app.db", "db-path")
        assert "system path detected" in str(exc.value).lower()

    def test_protected_system_bin(self):
        with pytest.raises(PermissionError) as exc:
            fd.check_path_safe("/bin/sh.db", "db-path")
        assert "system path detected" in str(exc.value).lower()

    def test_permission_error_message_format(self):
        with pytest.raises(PermissionError) as exc:
            fd.check_path_safe("/etc/foo", "log-path")
        assert "log-path" in str(exc.value)
        assert "/etc/foo" in str(exc.value)


# ─────────────────────────────────────────────────────────────────
# 3. HTML Parsing
# ─────────────────────────────────────────────────────────────────

class TestParseFloat:
    def test_positive_number(self):
        assert fd.parse_float("1234.56") == 1234.56

    def test_with_commas(self):
        assert fd.parse_float("4,291.90") == 4291.90

    def test_with_percent(self):
        assert fd.parse_float("-6.19%") == -6.19

    def test_with_dollar(self):
        assert fd.parse_float("$1,234.56") == 1234.56

    def test_invalid_returns_none(self):
        assert fd.parse_float("") is None
        assert fd.parse_float("abc") is None


class TestParsePriceAndChange:
    def test_price_change_percent(self):
        price, change, chg_pct = fd.parse_price_and_change("4,291.90-283.00 (-6.19%)")
        assert price == 4291.90
        assert change == -283.00
        assert chg_pct == -6.19

    def test_simple_price_only(self):
        price, change, chg_pct = fd.parse_price_and_change("648.57")
        assert price == 648.57
        assert change is None
        assert chg_pct is None

    def test_percent_change_positive(self):
        price, change, chg_pct = fd.parse_price_and_change("642.91+5.66 (+0.87%)")
        assert price == 642.91
        assert change == 5.66
        assert chg_pct == 0.87


class TestParseHtmlContent:
    def _json_ld_html(self, ticker_symbol, price, price_change_pct="+1.50",
                       prev_price=None, price_change=None):
        prev = prev_price or (price - 10)
        pc = price_change or (price - prev)
        return f'''<!DOCTYPE html>
<html>
<head>
<script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "FinancialProduct",
  "tickerSymbol": "{ticker_symbol}",
  "price": "{price}",
  "priceChangePercent": "{price_change_pct}",
  "priceChange": "{pc:.2f}",
  "previousPrice": "{prev}"
}}
</script>
</head>
<body></body>
</html>'''

    def _strip_html(self, ticker_symbol, price, price_change_pct="+1.50",
                     prev_price=None):
        prev = prev_price or (price - 10)
        return f'''
<html><body>
<p>52 week range 4,000.00 - 5,500.00</p>
<p>Previous Close {prev}</p>
<p class="QuoteStrip-lastPrice">{price}</p>
<p>After Hours: <span>{price}</span></p>
</body></html>'''

    def test_json_ld_price_extraction(self):
        html = self._json_ld_html("SPY", 4521.30, "+1.23", prev_price=4468.0, price_change=53.3)
        result = fd.parse_html_content(html, "SPY")
        assert result["current_price"] == 4521.30
        assert result["change_pct"] == 1.23

    def test_json_ld_dxy_ticker(self):
        html = self._json_ld_html(".DXY", 104.55, "+0.25", prev_price=104.29, price_change=0.26)
        result = fd.parse_html_content(html, "DXY")
        assert result["current_price"] == 104.55
        assert result["change_pct"] == 0.25

    def test_json_ld_vix_ticker(self):
        html = self._json_ld_html(".VIX", 18.42, "-2.10", prev_price=18.81, price_change=-0.39)
        result = fd.parse_html_content(html, "VIX")
        assert result["current_price"] == 18.42

    def test_quote_strip_last_price(self):
        html = self._strip_html("SPY", 4521.30)
        result = fd.parse_html_content(html, "SPY")
        assert result["current_price"] == 4521.30

    def test_prev_close_from_html(self):
        html = '<html><body>Previous Close 4468.00</body></html>'
        result = fd.parse_html_content(html, "SPY")
        assert result["prev_close"] == 4468.0

    def test_week52_range(self):
        html = '<html><body>52 week range 4,000.00 - 5,500.00</body></html>'
        result = fd.parse_html_content(html, "SPY")
        assert result["week52_low"] == 4000.0
        assert result["week52_high"] == 5500.0

    def test_after_hours(self):
        html = '<html><body>After Hours:<span>4520.00</span></body></html>'
        result = fd.parse_html_content(html, "SPY")
        assert result["after_hours"] == 4520.0

    def test_price_change_pattern(self):
        html = '<html><body>4,291.90-283.00 (-6.19%)</body></html>'
        result = fd.parse_html_content(html, "SPY")
        assert result["current_price"] == 4291.90
        assert result["change_pct"] == -6.19

    def test_unknown_code_returns_defaults(self):
        html = '<html><body>Unknown ticker here</body></html>'
        result = fd.parse_html_content(html, "UNKNOWN")
        assert result["current_price"] is None

    def test_result_keys_always_present(self):
        html = '<html><body>empty</body></html>'
        result = fd.parse_html_content(html, "SPY")
        for key in ("prev_close", "current_price", "after_hours", "change_pct",
                    "week52_high", "week52_low", "dist_from_high_pct", "dist_from_low_pct"):
            assert key in result


# ─────────────────────────────────────────────────────────────────
# 4. Database Operations
# ─────────────────────────────────────────────────────────────────

class TestInitDb:
    def test_creates_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = fd.init_db(db_path)
            c = conn.cursor()
            # Check indicators table exists with correct columns
            c.execute("PRAGMA table_info(indicators)")
            cols = {row[1] for row in c.fetchall()}
            expected = {"id", "fetch_date", "fetch_time", "name_cn", "code",
                        "prev_close", "current_price", "after_hours", "change_pct",
                        "week52_high", "dist_from_high_pct", "week52_low",
                        "dist_from_low_pct", "source", "created_at"}
            assert expected.issubset(cols), f"Missing columns: {expected - cols}"

            # Check fetch_log table exists
            c.execute("PRAGMA table_info(fetch_log)")
            log_cols = {row[1] for row in c.fetchall()}
            assert "fetch_date" in log_cols
            assert "indicators_count" in log_cols
            conn.close()

    def test_idempotent_create(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn1 = fd.init_db(db_path)
            conn1.close()
            # Calling again should not raise
            conn2 = fd.init_db(db_path)
            conn2.close()

    def test_creates_parent_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "subdir", "nested", "test.db")
            conn = fd.init_db(db_path)
            assert os.path.exists(db_path)
            conn.close()

    def test_indexes_created(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = fd.init_db(db_path)
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='index'")
            index_names = {row[0] for row in c.fetchall()}
            assert "idx_code" in index_names
            assert "idx_fetch_time" in index_names
            conn.close()


class TestDbInsert:
    def test_insert_and_retrieve_indicator(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = fd.init_db(db_path)
            c = conn.cursor()
            c.execute(
                """INSERT INTO indicators
                (fetch_date, fetch_time, name_cn, code, prev_close, current_price,
                 after_hours, change_pct, week52_high, dist_from_high_pct,
                 week52_low, dist_from_low_pct, source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                ("2026-03-25", "2026-03-25 15:30", "美国10年期TIPS",
                 "US10YTIP", 4450.0, 4460.5, None, 0.24, 4800.0,
                 -7.08, 3900.0, 14.37, "cnbc")
            )
            conn.commit()
            c.execute("SELECT code, current_price, change_pct FROM indicators WHERE code=?", ("US10YTIP",))
            row = c.fetchone()
            assert row == ("US10YTIP", 4460.5, 0.24)
            conn.close()

    def test_insert_multiple_indicators(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = fd.init_db(db_path)
            c = conn.cursor()
            for i in range(5):
                c.execute(
                    "INSERT INTO indicators (fetch_date,fetch_time,name_cn,code,current_price,source) "
                    "VALUES (?,?,?,?,?,?)",
                    ("2026-03-25", "2026-03-25 15:30", f"Indicator {i}",
                     f"CODE{i}", 100.0 + i, "cnbc")
                )
            conn.commit()
            c.execute("SELECT COUNT(*) FROM indicators")
            assert c.fetchone()[0] == 5
            conn.close()

    def test_fetch_log_insert(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = fd.init_db(db_path)
            c = conn.cursor()
            c.execute(
                "INSERT INTO fetch_log (fetch_date,fetch_time,source,indicators_count) VALUES (?,?,?,?)",
                ("2026-03-25", "2026-03-25 15:30", "cnbc", 10)
            )
            conn.commit()
            c.execute("SELECT source, indicators_count FROM fetch_log")
            row = c.fetchone()
            assert row == ("cnbc", 10)
            conn.close()


# ─────────────────────────────────────────────────────────────────
# 5. INDICATORS Config Validation
# ─────────────────────────────────────────────────────────────────

class TestIndicatorsConfig:
    def test_all_have_required_keys(self):
        required_keys = {"code", "name_cn", "url"}
        for ind in fd.INDICATORS:
            assert required_keys.issubset(ind.keys()), \
                f"Indicator {ind.get('code','?')} missing keys: {required_keys - ind.keys()}"

    def test_all_codes_unique(self):
        codes = [ind["code"] for ind in fd.INDICATORS]
        assert len(codes) == len(set(codes)), "Duplicate codes found"

    def test_all_urls_are_cnbc(self):
        for ind in fd.INDICATORS:
            assert ind["url"].startswith("https://www.cnbc.com/"), \
                f"Indicator {ind['code']} URL not CNBC: {ind['url']}"

    def test_expected_codes_present(self):
        expected_codes = {
            "US10YTIP", "US10Y", "GC", "CL", "SPY",
            "SPX", "QQQ", "NDX", "DXY", "VIX",
        }
        actual_codes = {ind["code"] for ind in fd.INDICATORS}
        assert expected_codes == actual_codes, \
            f"Expected {expected_codes}, got {actual_codes}"

    def test_ten_indicators(self):
        assert len(fd.INDICATORS) == 10

    def test_no_empty_name_cn(self):
        for ind in fd.INDICATORS:
            assert ind["name_cn"], f"Empty name_cn for {ind['code']}"
            assert len(ind["name_cn"]) > 0

    def test_no_empty_code(self):
        for ind in fd.INDICATORS:
            assert ind["code"], "Empty code found"
            assert len(ind["code"]) > 0


# ─────────────────────────────────────────────────────────────────
# 6. fetch_html / fetch_cnbc with mock
# ─────────────────────────────────────────────────────────────────

class TestFetchHtml:
    @mock.patch("fetch_data.urlopen")
    def test_fetch_html_success(self, mock_urlopen):
        mock_resp = mock.MagicMock()
        mock_resp.read.return_value = b"<html><body>42</body></html>"
        mock_resp.__enter__ = mock.MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        html = fd.fetch_html("https://www.cnbc.com/quotes/SPY")
        assert "<html>" in html
        mock_urlopen.assert_called_once()

    @mock.patch("fetch_data.urlopen")
    def test_fetch_html_retries_on_5xx(self, mock_urlopen):
        # First two calls raise 503, third succeeds
        mock_fail = mock.MagicMock()
        mock_fail.__enter__ = mock.MagicMock(side_effect=Exception("should not reach"))
        mock_fail.__exit__ = mock.MagicMock(return_value=False)

        err_503 = Exception("503")
        err_503.code = 503

        mock_success = mock.MagicMock()
        mock_success.read.return_value = b"<html>ok</html>"
        mock_success.__enter__ = mock.MagicMock(return_value=mock_success)
        mock_success.__exit__ = mock.MagicMock(return_value=False)

        mock_urlopen.side_effect = [err_503, err_503, mock_success]

        with warnings.catch_warnings(record=True):
            html = fd.fetch_html("https://www.cnbc.com/quotes/SPY", retries=3)
        assert "<html>ok</html>" in html
        assert mock_urlopen.call_count == 3

    @mock.patch("fetch_data.urlopen")
    def test_fetch_html_raises_fetch_error_on_4xx(self, mock_urlopen):
        from urllib.error import HTTPError
        mock_urlopen.side_effect = HTTPError(
            url="https://www.cnbc.com/quotes/SPY",
            code=404,
            msg="Not Found",
            hdrs={},
            fp=None
        )
        with pytest.raises(fd.FetchError) as exc:
            fd.fetch_html("https://www.cnbc.com/quotes/SPY")
        assert "404" in str(exc.value)

    @mock.patch("fetch_data.urlopen")
    def test_fetch_html_rate_limit_429_respects_retry_after(self, mock_urlopen):
        from urllib.error import HTTPError
        headers = {"Retry-After": "30"}
        err_429 = HTTPError(
            url="https://www.cnbc.com/quotes/SPY",
            code=429,
            msg="Too Many Requests",
            hdrs=headers,
            fp=None
        )
        mock_success = mock.MagicMock()
        mock_success.read.return_value = b"<html>ok</html>"
        mock_success.__enter__ = mock.MagicMock(return_value=mock_success)
        mock_success.__exit__ = mock.MagicMock(return_value=False)

        mock_urlopen.side_effect = [err_429, mock_success]

        with warnings.catch_warnings(record=True):
            html = fd.fetch_html("https://www.cnbc.com/quotes/SPY", retries=2)
        assert "<html>ok</html>" in html


class TestFetchCnbc:
    @mock.patch("fetch_data.fetch_html")
    def test_fetch_cnbc_returns_dict_with_code_and_name_cn(self, mock_fetch_html):
        mock_fetch_html.return_value = '<html><body>No price</body></html>'
        result = fd.fetch_cnbc({"code": "SPY", "name_cn": "标普500 ETF",
                                 "url": "https://www.cnbc.com/quotes/SPY"})
        assert result["code"] == "SPY"
        assert result["name_cn"] == "标普500 ETF"
        assert result["source"] == "cnbc"

    @mock.patch("fetch_data.fetch_html")
    def test_fetch_cnbc_no_price_warns(self, mock_fetch_html):
        mock_fetch_html.return_value = '<html><body>nothing</body></html>'
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = fd.fetch_cnbc({"code": "SPY", "name_cn": "标普500 ETF",
                                     "url": "https://www.cnbc.com/quotes/SPY"})
            assert "No price extracted" in str(w[0].message)

    @mock.patch("fetch_data.fetch_html")
    def test_fetch_cnbc_fetch_error_returns_none(self, mock_fetch_html):
        mock_fetch_html.side_effect = fd.FetchError("boom")
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = fd.fetch_cnbc({"code": "SPY", "name_cn": "标普500 ETF",
                                     "url": "https://www.cnbc.com/quotes/SPY"})
            assert result is None
