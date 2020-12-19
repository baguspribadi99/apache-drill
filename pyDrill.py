from pydrill.client import PyDrill

drill = PyDrill(host='localhost', port=8047)

if not drill.is_active():
    raise ImproperlyConfigured('Please run Drill first')

def test_query() :
  result = drill.query('''
    SELECT * FROM api.covid.`daily/12-5-2020`
  ''')
      
  return result


# pandas dataframe

# df = yelp_reviews.to_dataframe()
# print(df[df['stars'] > 3])