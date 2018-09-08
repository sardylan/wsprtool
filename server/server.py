import json
import logging
import re

import flask
import psycopg2
from flask import Response

_logger = logging.getLogger(__name__)


class WSPRServer:
    def __init__(self, db_conn_str, port):
        self._db_conn_str = db_conn_str
        self._port = port

        self._app = None

    def start(self):
        self._preprare_app()
        self._prepare_routing()
        self._start_app()

    def _preprare_app(self):
        self._app = flask.Flask("WSPR")

    def _prepare_routing(self):
        self._app.add_url_rule(rule="/api/v1/wsprspots/count",
                               view_func=self._rule_wsprspots_count,
                               methods=["GET"])
        self._app.add_url_rule(rule="/api/v1/wsprspots/get_callsign/<string:callsign>",
                               view_func=self._rule_wsprspots_get_callsign,
                               methods=["GET"])

    def _start_app(self):
        self._app.run(
            port=self._port,
            threaded=True
        )

    def _rule_wsprspots_count(self):
        conn = self._preprare_dbconn()
        cursor = conn.cursor()

        sql = "SELECT COUNT(id) FROM wsprspots;"
        cursor.execute(sql)
        rows = cursor.fetchall()
        count = rows[0][0]

        conn.close()

        return self._preprare_response({
            "success": True,
            "count": count
        })

    def _rule_wsprspots_get_callsign(self, callsign=""):
        callsign = re.sub("[^A-Z0-9\-/\\\\]+", "", str(callsign.upper()))

        conn = self._preprare_dbconn()
        cursor = conn.cursor()

        sql = "SELECT id, datetime, reporter, reporter_grid, snr, frequency, callsign, grid, power, drift, distance, azimuth, band, sw_ver, code " \
              "FROM wsprspots ws " \
              "WHERE ws.callsign = %s " \
              "ORDER BY datetime DESC " \
              "LIMIT 20;"
        cursor.execute(sql, (callsign,))
        rows = cursor.fetchall()

        spots = [{
            "spot_id": int(row[0]),
            "timestamp": int(row[1].timestamp()),
            "reporter": row[2],
            "reporter_grid": row[3],
            "snr": int(row[4]),
            "frequency": int(row[5]),
            "call_sign": row[6],
            "grid": row[7],
            "power": int(row[8]),
            "drift": int(row[9]),
            "distance": int(row[10]),
            "azimuth": int(row[11]),
            "band": int(row[12]),
            "version": row[13],
            "code": int(row[14])
        } for row in rows]

        conn.close()

        return self._preprare_response({
            "success": True,
            "spots": spots
        })

    def _preprare_response(self, response):
        return Response(
            response=json.dumps(response),
            mimetype="application/json"
        )

    def _preprare_dbconn(self):
        return psycopg2.connect(self._db_conn_str)
