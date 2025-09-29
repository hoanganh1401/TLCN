import logging
import os
import json

from dags.transform_waqi_to_csv import _extract_row_from_payload, write_row_to_csv


SAMPLE = {
  "status": "ok",
  "data": {
    "aqi": 11,
    "idx": 12961,
    "attributions": [
      {
        "url": "http://cem.gov.vn/",
        "name": "Vietnam Center For Environmental Monitoring Portal (cổng thông tin quan trắc môi trường)",
        "logo": "Vietnam-CEM.png"
      },
      {
        "url": "https://waqi.info/",
        "name": "World Air Quality Index Project"
      }
    ],
    "city": {
      "geo": [
        21.1873402,
        106.0742958
      ],
      "name": "Bắc Ninh/TT Quan trắc - phường Suối Hoa - TP Bắc Ninh, Vietnam",
      "url": "https://aqicn.org/city/vietnam/bac-ninh/tt-quan-trac-phuong-suoi-hoa-tp-bac-ninh",
      "location": ""
    },
    "dominentpol": "pm25",
    "iaqi": {
      "co": {"v": 1},
      "dew": {"v": 25},
      "h": {"v": 97},
      "o3": {"v": 34},
      "p": {"v": 1004},
      "pm10": {"v": 7},
      "pm25": {"v": 11},
      "t": {"v": 25.5},
      "w": {"v": 7.7}
    },
    "time": {"s": "2025-09-29 16:00:00", "tz": "+07:00", "v": 1759161600, "iso": "2025-09-29T16:00:00+07:00"},
    "forecast": {},
    "debug": {"sync": "2025-09-29T18:15:11+09:00"}
  }
}


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('test_transform')

    row = _extract_row_from_payload(SAMPLE)
    out_dir = os.path.join(os.path.dirname(__file__), '..', 'dags', 'Processed-data')
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    file_path, written = write_row_to_csv(row, output_base=os.path.join(os.path.dirname(__file__), '..', 'dags', 'Processed-data'), logger=logger)
    logger.info(f"Test finished: written={written}, file={file_path}")


if __name__ == '__main__':
    main()
