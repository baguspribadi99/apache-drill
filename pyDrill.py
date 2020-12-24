from flask import jsonify
from pydrill.client import PyDrill
from pydrill.exceptions import ImproperlyConfigured
import json

drill = PyDrill(host='128.199.87.104', port=8047, auth='ihsan:kelompoK16A')


# drill = PyDrill(host='localhost', port=8047)
# yesterday = datetime.now() - timedelta(1)
# yesterday_string = datetime.strftime(yesterday, '%Y-%m-%d')
# print(yesterday_string)


def home_res():
    response = [
        {
            'code': 200,
            'status': 'success'
        }

    ]
    return jsonify({'response': response})


# def test_query():
#     # drill.is_active()
#     if not drill.is_active():
#         raise ImproperlyConfigured('Please run Drill first')
#
#     query = "select t.`date`, t.iso_code, t.location, t.total_cases, h.Population from " \
#             "hdfs.root.`owid-covid-data.csv` AS t INNER JOIN `hdfs.root`.`population_by_country.csv` AS h ON " \
#             "h.Country = t.location where " \
#             "t.location='Indonesia' AND t.`date`='" + yesterday_string + "' "
#     country = drill.query(query)
#     df = country.to_dataframe()
#     df = json.loads(df.to_json(orient='table'))
#
#     return df


# def get_all_countries_query():
#     # drill.is_active()
#     if not drill.is_active():
#         raise ImproperlyConfigured('Please run Drill first')
#
#     query = "SELECT countrytbl.res.name name, countrytbl.res.code code FROM (SELECT FLATTEN(result) res FROM " \
#             "api.country.`countries`) countrytbl "
#     #
#     country = drill.query(query)
#
#     res = {"result": []}
#     for i in country:
#         res["result"].append(i)
#
#     # df = country.to_dataframe()
#     # df = json.loads(df.to_json(orient='table'))
#
#     # print(res["result"])
#
#     result = json.dumps(res)
#
#     return result

def countries_res():
    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    query = "select distinct t.iso_code, t.location from hdfs.root.`owid-covid-data.csv` AS t"
    country = drill.query(query)
    df = country.to_dataframe()
    df = df.sort_values('location')
    df = json.loads(df.to_json(orient='table'))

    return df


def get_detail_indonesia_query():
    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    # Query using CROSS JOIN query = "SELECT * FROM (SELECT idncovid.`update`.`penambahan`.`jumlah_positif`
    # TotalConfirmed, idncovid.`update`.`penambahan`.`jumlah_meninggal` TotalDeaths,
    # idncovid.`update`.`penambahan`.`jumlah_sembuh` TotalRecovered, idncovid.`update`.`penambahan`.`jumlah_dirawat`
    # TotalHospitalised FROM api.covidid.`update.json` idncovid) tbl1 CROSS JOIN (SELECT
    # idcovid.`har`.`key_as_string` `date`, idcovid.`har`.`jumlah_positif`.`value` `NewConfirmed` FROM (SELECT
    # FLATTEN(idncovid.`update`.`harian`) har FROM api.covidid.`update.json` idncovid) idcovid) tbl2"
    query = "SELECT idcovid.`har`.`key_as_string` `Tanggal`, idcovid.`har`.`jumlah_positif`.`value` `NewConfirmed`, " \
            "idcovid.`har`.`jumlah_positif_kum`.`value` `TotalConfirmed`,  idcovid.`har`.`jumlah_sembuh_kum`.`value` " \
            "`TotalRecovered`, " \
            "idcovid.`har`.`jumlah_meninggal_kum`.`value` `TotalDeaths`, idcovid.`har`.`jumlah_dirawat_kum`.`value` " \
            "`TotalHospitalized` FROM (SELECT FLATTEN(idncovid.`update`.`harian`) har FROM api.covid.`update.json` " \
            "idncovid) idcovid "
    # Enable CROSS JOIN. For more info please refer to this link https://drill.apache.org/docs/from-clause/
    # enable_cross_join = drill.query("set `planner.enable_nljoin_for_scalar_only` = false")

    # Querying data
    idn_covid_data = drill.query(query)

    # res = {"result":[]}
    # for i in idn_covid_data:
    #     # print(i)
    #     # print(type(i))
    #     res["result"].append(i)

    # df = country.to_dataframe()
    # df = json.loads(df.to_json(orient='table'))

    # print(res["result"])

    # result = json.dumps(res)
    df = idn_covid_data.to_dataframe()
    df["Tanggal"] = df["Tanggal"].str.replace("T00:00:00.000Z", "")
    df = json.loads(df.to_json(orient='table'))

    return df


def get_detail_country_query(iso):
    # if country=="IDN":
    #     return 

    # drill.is_active()
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    # query = "SELECT columns[4] AS TotalConfirmed, columns[7] AS TotalDeaths, " \
    #         "columns[19] AS TotalHospitalized, columns[3] as Tanggal, columns[5] AS NewConfirmed " \
    #         "FROM dfs.`/tmp/data/owid-covid-data.csv` " \
    #         "WHERE columns[0]='" + iso + "' " \
    # "AND columns[3]='" + yesterday_string + "'"

    # hdfs version
    query = "SELECT t.total_cases AS TotalConfirmed, t.total_deaths AS TotalDeaths, " \
            "t.hosp_patients AS TotalHospitalized, t.`date` as Tanggal, t.new_cases AS NewConfirmed, " \
            "t.total_cases_per_million AS CasesPerMillion FROM hdfs.root.`owid-covid-data.csv` AS t " \
            "WHERE t.iso_code='" + iso + "' " \

    country = drill.query(query)
    res = {"result": []}
    for i in country:
        res["result"].append(i)

    result = json.dumps(res)

    return result


def get_indonesia_cases_per_million():
    if not drill.is_active():
        raise ImproperlyConfigured('Please run Drill first')

    # hdfs version
    query = "SELECT t.total_cases_per_million AS CasesPerMillion FROM hdfs.root.`owid-covid-data.csv` AS t WHERE " \
            "t.iso_code='IDN' "
    indonesia = drill.query(query)
    df = indonesia.to_dataframe()
    df = df.tail(1)
    df = json.loads(df.to_json(orient='table'))

    return df
