from flask import Flask, render_template, request, redirect, url_for

from webapp.helper import TableServiceHelper

app = Flask(__name__, static_folder="../static", template_folder="../templates")


@app.route("/")
def index():
    entity_list = TableServiceHelper().query_entity(request.args)
    result = TableServiceHelper.serializer(entity_list)
    result["args"] = request.args
    return render_template('index.html', **result)


@app.route("/api/entity", methods=["POST"])
def handler_entity_action():
    action = request.args.get("action")
    if action == "delete":
        TableServiceHelper().delete_entity()
    elif action in ["insert", "insertCustom"]:
        TableServiceHelper().insert_entity()
    elif action in ["upsert", "upsetCustom"]:
        TableServiceHelper().upsert_entity()
    elif action == "update":
        TableServiceHelper().update_entity()
    elif action == "insertSampleData":
        TableServiceHelper().insert_sample_data()
    return redirect(url_for("index"))
