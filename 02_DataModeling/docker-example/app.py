#Example Docker App using this resource:
#https://docs.docker.com/language/python/build-images/

from flask import Flask
#Flask intro: https://flask.palletsprojects.com/en/2.1.x/quickstart/
#Flask allows us to create a web server (like Express.js?), it seems
#Note: "localhost resolves to 127.0.0.1"

#Note: __name__ gives information on the execution context
#If __name__ == __main__, this is the primary script. Else, it's a support script.
app = Flask(__name__)

@app.route('/')
def hello_world():
    return "huhu!"
    #python3 -m flask run
    #Nav to http://localhost:5000/
    #Default port is 5000
