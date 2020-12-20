from pydrill.client import PyDrill
from pydrill.exceptions import ImproperlyConfigured
import json
from datetime import datetime, timedelta

drill = PyDrill(host='139.59.108.226', port=8047)
yesterday = datetime.now() - timedelta(1)
yesterday_string = datetime.strftime(yesterday, '%Y-%m-%d')
print(yesterday_string)


def test_query():
    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    query = "select t.`date`, t.iso_code, t.location, t.total_cases, h.Population from " \
            "hdfs.root.`owid-covid-data.csv` AS t INNER JOIN `hdfs.root`.`population_by_country.csv` AS h ON " \
            "h.Country = t.location where " \
            "t.location='Indonesia' AND t.`date`='" + yesterday_string + "' "
    country = drill.query(query)
    df = country.to_dataframe()
    df = json.loads(df.to_json(orient='table'))

    return df
