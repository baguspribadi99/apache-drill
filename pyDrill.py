from pydrill.client import PyDrill
from pydrill.exceptions import ImproperlyConfigured
import json
from datetime import datetime, timedelta
from flask import make_response

# drill = PyDrill(host='139.59.108.226', port=8047)
drill = PyDrill(host='localhost', port=8047)
yesterday = datetime.now() - timedelta(1)
yesterday_string = datetime.strftime(yesterday, '%Y-%m-%d')
# print(yesterday_string)


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

def get_idn_covid_data():
    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    main_dict = {"status":200,"result":[]}
    result_list = []
    result_dict = {"code":"ID"}
    today_dict = {"Date":datetime.today().strftime('%Y-%m-%d')}
    daily_dict = {"daily":[]}

        
    covid_total_data_query = "SELECT idncovid.`update`.`penambahan`.`jumlah_positif` TotalConfirmed, idncovid.`update`.`penambahan`.`jumlah_meninggal` TotalDeaths, idncovid.`update`.`penambahan`.`jumlah_sembuh` TotalRecovered, idncovid.`update`.`penambahan`.`jumlah_dirawat` TotalHospitalised FROM api.covidid.`update.json` idncovid"
    covid_daily_data_query = "SELECT idcovid.`har`.`key_as_string` `date`, idcovid.`har`.`jumlah_positif`.`value` `NewConfirmed` FROM (SELECT FLATTEN(idncovid.`update`.`harian`) har FROM api.covidid.`update.json` idncovid) idcovid"

    covid_total_data = drill.query(covid_total_data_query)
    covid_daily_data = drill.query(covid_daily_data_query)

    for i in covid_daily_data:
        # print(i)
        daily_dict["daily"].append(i)
    
    # Append covid_total_data to result_dict
    for d in covid_total_data:
        for k, v in d.items():  # d.items() in Python 3+
            result_dict.setdefault(k, v)
    
    # Append today's date to result_dict
    result_dict.setdefault("Date", datetime.today().strftime('%Y-%m-%d'))
    
    # Append daily data to result_dict
    result_dict["daily"] = daily_dict["daily"]
    
    # Append all to main_list
    result_list.append(result_dict)
    main_dict["result"] = result_list

    # Converting main_list to JSON
    result = json.dumps(main_dict)
    
    return result

def coba_coba():
    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    # Query using CROSS JOIN
    query = "SELECT * FROM (SELECT idncovid.`update`.`penambahan`.`jumlah_positif` TotalConfirmed, idncovid.`update`.`penambahan`.`jumlah_meninggal` TotalDeaths, idncovid.`update`.`penambahan`.`jumlah_sembuh` TotalRecovered, idncovid.`update`.`penambahan`.`jumlah_dirawat` TotalHospitalised FROM api.covidid.`update.json` idncovid) tbl1 CROSS JOIN (SELECT idcovid.`har`.`key_as_string` `date`, idcovid.`har`.`jumlah_positif`.`value` `NewConfirmed` FROM (SELECT FLATTEN(idncovid.`update`.`harian`) har FROM api.covidid.`update.json` idncovid) idcovid) tbl2"

    # Enable CROSS JOIN. For more info please refer to this link https://drill.apache.org/docs/from-clause/
    enable_cross_join = drill.query("set `planner.enable_nljoin_for_scalar_only` = false")

    # Querying data
    idn_covid_data = drill.query(query)

    res = {"result":[]}
    for i in idn_covid_data:
        # print(i)
        # print(type(i))
        res["result"].append(i)

    # df = country.to_dataframe()
    # df = json.loads(df.to_json(orient='table'))

    # print(res["result"])

    result = json.dumps(res)

    return result 

def get_all_countries_query():
    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')


    query = "SELECT countrytbl.res.code code FROM (SELECT FLATTEN(result) res FROM api.country.`countries`) countrytbl WHERE countrytbl.res.name='Indonesia'"
    
    country = drill.query(query)
    # print(country)
    res = {"result":[]}
    for i in country:
        # print(i)
        # print(type(i))
        res["result"].append(i)

    # df = country.to_dataframe()
    # df = json.loads(df.to_json(orient='table'))

    # print(res["result"])

    result = json.dumps(res)

    return result