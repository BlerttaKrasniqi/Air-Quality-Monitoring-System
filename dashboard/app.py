from flask import Flask, render_template
from cassandra.cluster import Cluster
import pandas

app = Flask(__name__)

@app.route("/") 
def index():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('air_monitoring')

    rows = session.execute("SELECT * FROM sensor_data LIMIT 100;")

    data = pandas.DataFrame(rows, columns=['id', 'pm25', 'pm10', 'co2', 'temperature', 'humidity', 'timestamp'])

    if not data.empty:
        data=data.sort_values(by='timestamp',ascending=False)
        data['timestamp'] = data['timestamp'].astype(str)
    
    records = data.to_dict(orient='records')

  

    return render_template("index.html", data=records)  

if __name__ == "__main__":
    app.run(debug=True)
