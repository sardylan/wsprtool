import csv
import datetime
import getopt
import gzip
import os
import sys

import psycopg2

MODE_IMPORT = "import"
MODE_SERVE = "serve"

IMPORT_LOG_INTERVAL = 1000


class WSPRTool:
    def __init__(self):
        self._mode = MODE_SERVE

        self._file = ""

    def main(self):
        self.check_opt()

        self.print_config()

        if self._mode == MODE_SERVE:
            self.start_serve()
        elif self._mode == MODE_IMPORT:
            self.start_import()

    def check_opt(self):
        try:
            opts, args = getopt.getopt(
                sys.argv[1:],
                "hisy:m:",
                ["help", "import", "serve", "file="]
            )
        except getopt.GetoptError as err:
            sys.stderr.write(str(err))
            self.usage()
            sys.exit(1)

        for param, value in opts:
            if param in ("-h", "--help"):
                self.usage()
                sys.exit(0)
            elif param in ("-i", "--import"):
                self._mode = MODE_IMPORT
            elif param in ("-s", "--serve"):
                self._mode = MODE_SERVE
            elif param in ("-f", "--file"):
                self._file = value

    def print_config(self):
        sys.stderr.write("Mode: %s\n" % self._mode)

        if self._mode == MODE_IMPORT:
            sys.stderr.write("File: %s\n" % self._file)

        sys.stderr.write("\n")

    def start_serve(self):
        sys.stderr.write("Starting serving data\n")

    def start_import(self):
        sys.stderr.write("Starting importing data\n")

        if not os.path.isfile(self._file):
            self.error("File \"%s\" not found\n" % self._file)

        fd = open(self._file, "rb")
        gzip_content = fd.read()
        fd.close()

        sys.stderr.write("File size: %d\n" % len(gzip_content))

        raw_content = gzip.decompress(gzip_content)
        sys.stderr.write("Decompressed size: %d\n" % len(raw_content))

        raw_lines = [x.decode() for x in raw_content.splitlines()]
        count_lines = len(raw_lines)
        sys.stderr.write("Lines to import: %d\n" % count_lines)

        csv_content = csv.reader(raw_lines)

        connection_string = "host='127.0.0.1' port=5432 user='wspr' password='wspr' dbname='wspr'"
        conn = psycopg2.connect(connection_string)

        cursor = conn.cursor()

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

        count = 0
        count_insert = 0
        count_update = 0
        ts_start = datetime.datetime.now()

        try:
            for row in csv_content:
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

                db_function = "wsprspots_insert"
                if rows[0][0] > 0:
                    db_function = "wsprspots_update"

                if db_function == "wsprspots_insert":
                    count_insert += 1
                if db_function == "wsprspots_update":
                    count_update += 1

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

                if count % IMPORT_LOG_INTERVAL == 0:
                    ts_end = datetime.datetime.now()

                    ts_delta = ts_end - ts_start
                    time_avg = ((ts_delta.seconds * 1000000) + ts_delta.microseconds) / IMPORT_LOG_INTERVAL

                    count_remaining = count_lines - count
                    count_percentage = float(count / count_lines) * 100

                    time_remaining = time_avg * count_remaining
                    time_remaining_delta = datetime.timedelta(microseconds=time_remaining)
                    time_remaining_str = str(time_remaining_delta)

                    eta = datetime.datetime.now() + time_remaining_delta
                    eta_str = str(eta)

                    sys.stderr.write("Line %d of %s ("
                                     "inserts: %d - "
                                     "update: %d - "
                                     "Progress: %.01f%% - "
                                     "Avg time per row: %.03f ms - "
                                     "Remaining: %s - "
                                     "ETA: %s"
                                     ")\n" % (
                                         count, count_lines,
                                         count_insert,
                                         count_update,
                                         count_percentage,
                                         float(time_avg / 1000),
                                         time_remaining_str,
                                         eta_str
                                     ))

                    count_insert = 0
                    count_update = 0
                    ts_start = datetime.datetime.now()

                    conn.commit()

                count += 1
        except psycopg2.Error as e:
            sys.stderr.write("\n")
            sys.stderr.write("ERROR importing row: %d - %s\n" % e.pgcode, e.pgerror)
            sys.stderr.write("SQL: %s\n" % sql)
            sys.stderr.write("\n")
            sys.stderr.write("\n")
            sys.stderr.write(
                "Line %d of %s (inserts: %d - update: %d)\n" % (count, count_lines, count_insert, count_update))
            sys.stderr.write("\n")
            self.error()

        sys.stderr.write(
            "Line %d of %s (inserts: %d - update: %d)\n" % (count, count_lines, count_insert, count_update))

        conn.commit()
        conn.close()

        sys.stderr.write("\n")
        sys.stderr.write("Import completed\n")
        sys.stderr.write("\n")

    @staticmethod
    def error(message=""):
        sys.stderr.write("\n")

        if message:
            sys.stderr.write("\n")
            sys.stderr.write("ERROR!!!\n")
            sys.stderr.write("%s\n" % message)
            sys.stderr.write("\n")

        sys.exit(1)

    @staticmethod
    def usage():
        sys.stderr.write("\n")
        sys.stderr.write("Usage: %s [options]\n" % sys.argv[0])
        sys.stderr.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write(" -h | --help                     Shows this message\n")
        sys.stderr.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write("Mode:\n")
        sys.stderr.write("\n")
        sys.stderr.write(" -i | --import                   Import data into database\n")
        sys.stderr.write(" -s | --serve                    Start web server for data query (default)\n")
        sys.stderr.write("\n")
        sys.stderr.write("\n")
        sys.stderr.write("Import mode options:\n")
        sys.stderr.write("\n")
        sys.stderr.write(" -i | --year=<year>              Year of import (must be 4 digits)\n")
        sys.stderr.write("                                   default to current year\n")
        sys.stderr.write(" -o | --month=<month>            Month of import\n")
        sys.stderr.write("                                   default to current month\n")
        sys.stderr.write("\n")
        sys.stderr.write("\n")


if __name__ == "__main__":
    wspr_tool = WSPRTool()
    wspr_tool.main()
