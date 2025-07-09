from flask import Flask, render_template
from cassandra.cluster import Cluster
import pandas

app = Flask(__name__)

@app.route("/")  # ✅ Add this line
def index():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('air_monitoring')

    rows = session.execute("SELECT * FROM sensor_data LIMIT 100;")

    data = pandas.DataFrame(rows, columns=['id', 'pm25', 'pm10', 'co2', 'temperature', 'humidity', 'timestamp'])
    data = data.sort_values(by='timestamp', ascending=False)

    return render_template("index.html", data=data)  # make sure to pass data if needed

if __name__ == "__main__":
    app.run(debug=True)
