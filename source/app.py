import flask
import json
import pathlib

import jobmanager

ROOT = pathlib.Path(__file__).parents[1].resolve()
SOURCE_ROOT = ROOT / 'source'
app = flask.Flask("CoSI", root_path=SOURCE_ROOT)
MANAGER = jobmanager.JobManager()

@app.route("/")
def index():
    """
    show the information page
    """
    return flask.send_from_directory("static", "main.html")

@app.route("/manage")
def manage():
    """
    show a page with running jobs incl option to terminate them
    """
    jobs, services, recent = MANAGER.list_jobs()
    return flask.render_template("manage.html", jobs=jobs, services=services, recent=recent)

@app.route("/terminate")
def terminate():
    """
    terminate a job by id or all jobs of a service by service name
    """
    try:
        job_id = flask.request.args["id"]
    except KeyError:
        message = {"error": "no 'id' argument in request"}
        return flask.jsonify(message)

    result = MANAGER.kill_job(job_id)
    return flask.render_template("terminate.html", result=result)

@app.route("/log")
def log():
    """
    show the log of a job by id
    """
    try:
        job_id = flask.request.args["id"]
    except KeyError:
        message = {"error": "no 'id' argument in request"}
        return flask.jsonify(message)

    result = MANAGER.get_log(job_id)
    return flask.render_template("log.html", result=result)

@app.route("/info")
def info():
    """
    show the info page of a service
    """
    try:
        service_name = flask.request.args["service"]
    except KeyError:
        message = {"error": "no 'service' argument in request"}
        return flask.jsonify(message)

    info_text = MANAGER.get_info(service_name)
    return flask.render_template("service_info.html", service=service_name, info_text=info_text)

@app.route("/service", methods=['GET', 'POST'])
def service():
    """
    entry point to start processing a service request
    """
    if flask.request.method == 'POST':
        args = flask.request.form or flask.request.json
    else:
        args = flask.request.args

    try:
        args["service"]
    except KeyError:
        message = {"error": "no 'service' argument in request"}
        return flask.jsonify(message)

    data = dict(args)
    print('coming request: {}'.format(data))
    result = MANAGER.run_job(data)
    print('returning data')

    return flask.jsonify(result)

if __name__ == "__main__":
    app.run(threaded=True, host='0.0.0.1', port=8000)
