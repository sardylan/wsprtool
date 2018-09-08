import getopt
import logging
import sys

from importer.importer import WSPRImporter
from server.server import WSPRServer

MODE_IMPORT = "import"
MODE_SERVE = "serve"

IMPORT_LOG_INTERVAL = 1000

logging.basicConfig(
    format="%(asctime)s [%(levelname)5s] - %(message)s",
    level=logging.DEBUG,
    datefmt="%Y%m%d-%H%M%S"
)

_logger = logging.getLogger(__name__)


class WSPRTool:
    def __init__(self):
        self._mode = MODE_SERVE

        self._file = ""
        self._update = False

        self._port = 13254
        self._db_conn_str = "host='127.0.0.1' port=5432 user='wspr' password='wspr' dbname='wspr'"

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
                "hisy:f:up:",
                [
                    "help", "import", "serve",
                    "file=", "update",
                    "port="
                ]
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
            elif param in ("-u", "--update"):
                self._update = True
            elif param in ("-p", "--port"):
                self._port = int(value)

    def print_config(self):
        _logger.info("Mode: %s" % self._mode)

        if self._mode == MODE_IMPORT:
            _logger.info("File: %s" % self._file)

    def start_serve(self):
        server = WSPRServer(self._db_conn_str, self._port)
        server.start()

    def start_import(self):
        importer = WSPRImporter(self._db_conn_str, self._file, self._update)
        importer.start()

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
        sys.stderr.write(" -i | --file=<filename>          Filename to import (*.csv.gz)\n")
        sys.stderr.write(" -o | --update                   Enable record update\n")
        sys.stderr.write("                                   default disabled\n")
        sys.stderr.write("\n")
        sys.stderr.write("\n")


if __name__ == "__main__":
    wspr_tool = WSPRTool()
    wspr_tool.main()
