DROP TABLE IF EXISTS wsprspots;

CREATE TABLE IF NOT EXISTS wsprspots
(
  id            bigserial PRIMARY KEY       NOT NULL,
  datetime      timestamp without time zone NOT NULL,
  reporter      varchar(24)                 NOT NULL,
  reporter_grid varchar(8)                  NOT NULL,
  snr           int                         NOT NULL,
  frequency     int                         NOT NULL,
  callsign      varchar(24)                 NOT NULL,
  grid          varchar(8)                  NOT NULL,
  power         int                         NOT NULL,
  drift         int                         NOT NULL,
  distance      int                         NOT NULL,
  azimuth       int                         NOT NULL,
  band          int                         NOT NULL,
  sw_ver        varchar(32)                 NOT NULL DEFAULT '',
  code          int                         NOT NULL
);

COMMENT ON COLUMN wsprspots.id
IS 'A unique integer identifying the spot which otherwise carries no information. Used as primary key in the database table. Not all spot numbers exist, and the files may not be in spot number order.';

COMMENT ON COLUMN wsprspots.datetime
IS 'The time of the spot in unix time() format (seconds since 1970-01-01 00:00 UTC). To convert to an excel date value, use =time_cell/86400+"1/1/70" and then format it as a date/time.';

COMMENT ON COLUMN wsprspots.reporter
IS 'The station reporting the spot. Usually an amateur call sign, but several SWLs have participated using other identifiers. Maximum of 10 characters.';

COMMENT ON COLUMN wsprspots.reporter_grid
IS 'Maidenhead grid locator of the reporting station, in 4- or 6-character format.';

COMMENT ON COLUMN wsprspots.snr
IS 'Signal to noise ratio in dB as reported by the receiving software. WSPR reports SNR referenced to a 2500 Hz bandwidth; typical values are -30 to +20dB.';

COMMENT ON COLUMN wsprspots.frequency
IS 'Frequency of the received signal in MHz.';

COMMENT ON COLUMN wsprspots.callsign
IS 'Call sign of the transmitting station. WSPR encoding of callsigns does not encode portable or other qualifying (slash) designators, so the call may not represent the true location of the transmitting station. Maximum of 6 characters.';

COMMENT ON COLUMN wsprspots.grid
IS 'Maidenhead grid locator of transmitting station, in 4- or 6-character format.';

COMMENT ON COLUMN wsprspots.power
IS 'Power, as reported by transmitting station in the transmission. Units are dBm (decibels relative to 1 milliwatt; 30dBm=1W). Typical values are 0-50dBm, though a few are negative (< 1 mW).';

COMMENT ON COLUMN wsprspots.drift
IS 'The measured drift of the transmitted signal as seen by the receiver, in Hz/minute. Mostly of use to make the transmitting station aware of systematic drift of the transmitter. Typical values are -3 to 3.';

COMMENT ON COLUMN wsprspots.distance
IS 'Approximate distance between transmitter and receiver along the great circle (short) path, in kilometers. Computed form the reported grid squares.';

COMMENT ON COLUMN wsprspots.azimuth
IS 'Approximate direction, in degrees, from transmitting station to receiving station along the great circle (short) path.';

COMMENT ON COLUMN wsprspots.band
IS 'Band of operation, computed from frequency as an index for faster retrieval. This may change in the future, but at the moment, it is just an integer representing the MHz component of the frequency with a special case for LF (-1: LF, 0: MF, 1: 160m, 3: 80m, 5: 60m, 7: 40m, 10: 30m, ...).';

COMMENT ON COLUMN wsprspots.sw_ver
IS 'Version string of the WSPR software in use by the receiving station. May be bank, as versions were not reported until version 0.6 or 0.7, and version reporting is only done through the realtime upload interface (not the bulk upload).';

COMMENT ON COLUMN wsprspots.code
IS 'Archives generated after 22 Dec 2010 have an additional integer Code field. Non-zero values will indicate that the spot is likely to be erroneous (bogus callsign, appears to be wrong band, appears to be an in-band mixing product, etc. When implemented, the specific codes will be documented here.';
