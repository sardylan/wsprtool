import csv
import datetime
import gzip
import logging
import os

import psycopg2

DB_FUNCTION_INSERT = "wsprspots_insert"
DB_FUNCTION_UPDATE = "wsprspots_update"

IMPORT_LOG_INTERVAL = 1000

_logger = logging.getLogger(__name__)


class WSPRImporter:
    def __init__(self, db_conn_str, file, enable_update):
        self._db_conn_str = db_conn_str
        self._file = file
        self._update = enable_update

        self._csv_content = None
        self._raw_lines = None

        self._lines = 0
        self._count = 0
        self._count_insert = 0
        self._count_update = 0

        self._time_avg = 0

    def start(self):
        if not os.path.isfile(self._file):
            _logger.error("File \"%s\" not found" % self._file)
            return

        self._preprare_lines()
        self._preprare_csv()
        self._preprare_dbconn()

        try:
            self._import_data()
        except psycopg2.Error as e:
            _logger.error("ERROR importing row %d of %d: %s - %s" % (self._count, self._lines, e.pgcode, e.pgerror))

        self._close_dbconn()

    def _preprare_lines(self):
        fd = open(self._file, "rb")
        gzip_content = fd.read()
        fd.close()
        _logger.debug("File size: %d" % len(gzip_content))

        raw_content = gzip.decompress(gzip_content)
        _logger.debug("Decompressed size: %d" % len(raw_content))

        self._raw_lines = [x.decode() for x in raw_content.splitlines()]
        self._lines = len(self._raw_lines)
        _logger.info("Lines to import: %d" % self._lines)

    def _preprare_csv(self):
        self._csv_content = csv.reader(self._raw_lines, escapechar='\\')

    def _preprare_dbconn(self):
        self._db_conn = psycopg2.connect(self._db_conn_str)

        cursor = self._db_conn.cursor()

        sql = "PREPARE wsprspots_exists AS " \
              "SELECT COUNT(id) FROM wsprspots WHERE id = $1;"
        cursor.execute(sql)

        sql = "PREPARE wsprspots_insert AS " \
              "INSERT INTO wsprspots (id, datetime, reporter, reporter_grid, snr, frequency, callsign, grid, power, drift, distance, azimuth, band, sw_ver, code) " \
              "VALUES ($1, to_timestamp($2)::timestamp without time zone, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);"
        cursor.execute(sql)

        sql = "PREPARE wsprspots_update AS " \
              "UPDATE wsprspots SET datetime = to_timestamp($2)::timestamp without time zone, reporter = $3, reporter_grid = $4, snr = $5, " \
              "frequency = $6, callsign = $7, grid = $8, power = $9, drift = $10, distance = $11, azimuth = $12, " \
              "band = $13, sw_ver = $14, code = $15 " \
              "WHERE id = $1;"
        cursor.execute(sql)

    def _close_dbconn(self):
        self._db_conn.close()

    def _import_data(self):
        cursor = self._db_conn.cursor()

        ts_start = datetime.datetime.now()

        for row in self._csv_content:
            values = {
                "spot_id": int(row[0]),
                "timestamp": int(row[1]),
                "reporter": row[2],
                "reporter_grid": row[3],
                "snr": int(row[4]),
                "frequency": int(float(row[5]) * 1000000),
                "call_sign": row[6],
                "grid": row[7],
                "power": int(row[8]),
                "drift": int(row[9]),
                "distance": int(row[10]),
                "azimuth": int(row[11]),
                "band": int(row[12]),
                "version": row[13],
                "code": int(row[14])
            }

            sql = "EXECUTE wsprspots_exists(%d);" % values["spot_id"]
            cursor.execute(sql)
            rows = cursor.fetchall()

            db_function = DB_FUNCTION_INSERT
            if rows[0][0] > 0:
                db_function = DB_FUNCTION_UPDATE

            parse_row = True
            if db_function == DB_FUNCTION_UPDATE and not self._update:
                parse_row = False

            if parse_row:
                if db_function == DB_FUNCTION_INSERT:
                    self._count_insert += 1
                elif db_function == DB_FUNCTION_UPDATE:
                    self._count_update += 1

                sql = "EXECUTE %s " \
                      "(%d, %d, '%s', '%s', %d, %d, '%s', '%s', %d, %d, %d, %d, %d, '%s', %d);" % (
                          db_function,
                          values["spot_id"],
                          values["timestamp"],
                          values["reporter"],
                          values["reporter_grid"],
                          values["snr"],
                          values["frequency"],
                          values["call_sign"],
                          values["grid"],
                          values["power"],
                          values["drift"],
                          values["distance"],
                          values["azimuth"],
                          values["band"],
                          values["version"],
                          values["code"]
                      )

                cursor.execute(sql)

            if self._count % IMPORT_LOG_INTERVAL == 0:
                self._db_conn.commit()

                ts_end = datetime.datetime.now()
                ts_delta = ts_end - ts_start
                self._time_avg = ((ts_delta.seconds * 1000000) + ts_delta.microseconds) / IMPORT_LOG_INTERVAL

                self._print_progress()

                ts_start = datetime.datetime.now()

            self._count += 1

        self._db_conn.commit()
        self._print_progress()

    def _print_progress(self):
        count_remaining = self._lines - self._count
        count_percentage = float(self._count / self._lines) * 100

        time_remaining = self._time_avg * count_remaining
        time_remaining_delta = datetime.timedelta(microseconds=time_remaining)
        time_remaining_str = str(time_remaining_delta)

        eta = datetime.datetime.now() + time_remaining_delta
        eta_str = str(eta)

        line_info = [
            "I:%d" % self._count_insert,
            "D:%d" % self._count_update,
            "Progress: %d%%" % count_percentage
        ]

        if count_percentage < 100:
            line_info.append("Avg time per row: %.03f ms" % float(self._time_avg / 1000))
            line_info.append("Remaining: %s" % time_remaining_str)
            line_info.append("ETA: %s" % eta_str)

        line = "Line %d of %d (%s)" % (self._count, self._lines, " - ".join(line_info))
        print(line)
