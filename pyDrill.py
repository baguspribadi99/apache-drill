from pydrill.client import PyDrill
import json

drill = PyDrill(host='localhost', port=8047)

if not drill.is_active():
    raise ImproperlyConfigured('Please run Drill first')

def test_query() :
  result = drill.query('''
    SELECT * FROM api.covid.`daily/12-5-2020`
  ''')
  result = result.to_dataframe()
  result = json.loads(result.to_json(orient="table"))
      
  return result


# pandas dataframe

# df = yelp_reviews.to_dataframe()
# print(df[df['stars'] > 3])