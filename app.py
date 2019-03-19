from flask import Flask, redirect, url_for, render_template, flash, request
from celery import Celery

import jsonpickle

app = Flask(__name__)

app.config["celery_helper_RESULT_BACKEND"] = "amqp"
app.config["celery_helper_BROKER_URL"] = "amqp://localhost//"
app.config["SECRET_KEY"] = "Thisismysecret"

def make_celery_helper(app):
	
	celery_helper = Celery(
		app.import_name,
		backend="rpc://",
		broker=app.config['celery_helper_BROKER_URL']
	)

	celery_helper.conf.update(app.config)

	class ContextTask(celery_helper.Task):
		def __call__(self, *args, **kwargs):
			with app.app_context():
				return self.run(*args, **kwargs)

	celery_helper.Task = ContextTask
	return celery_helper

celery_helper = make_celery_helper(app)

@app.route("/")
def index():
	return render_template("index.html")

@app.route("/upload", methods=["GET", "POST"])
def upload():
	if (request.method == "POST"):
		content = request.files["content"]
		filename = content.filename
		pickled_content = jsonpickle.encode(
			content)
		task_id = save_to_disk.delay(
			pickled_content, 
			filename)
		flash("Started upload for " + filename)
		return render_template("index.html", 
			task_id=task_id)

@app.route("/upload_status/<task_id>")
def get_task_status(task_id):
	task_state = celery_helper.AsyncResult(task_id).state
	flash("task status is "+task_state)
	return render_template(
		"index.html", 
		monitor=True, 
		task_id=task_id)

@app.route("/purge_upload/<task_id>")
def purge_task(task_id):
	celery_helper.control.revoke(task_id)
	flash("UPLOAD STOPPED!")
	return render_template("index.html", poisoned=True)

@celery_helper.task(name="app.save_to_disk")
def save_to_disk(pickled_content, filename):
	content = jsonpickle.decode(
		pickled_content)
	print(content)
	# content.stream = jsonpickle.decode(content.stream)
	print(content.stream)
	with open(content.filename, 'w') as f:
	    for item in content.stream:
	    	for i in range(0, 100000):
	        	f.write("%s\n" % item)

if __name__ == "__main__":
	app.run(debug=True)
