
# MetaSRA API/back-end

API/back-end for MetaSRA website.

This respository contains two important components: the Flask app for the MetaSRA API/back-end, and a Python script to build the Mongo database used by the API.



## Setup

When using any of this code on a new machine, do this first:

For development, "MetaSRA project directory" just means the directory you're using for the project.  For deployment, "MetaSRA project directory" is the directory from which you're serving the site (it should probably be /var/www.)

1. Install Python 3.x (developed on Python 3.5.2), and MongoDB 3.4 or higher.
2. Create or navigate to your MetaSRA directory, and create a virtual environment in a new directory called "ENV": `python3 -m venv ENV`. This is so the Python packages required by MetaSRA don't conflict with your globally-installed packages.
3. Git-clone this back-end repository into your MetaSRA project directory.
4. Activate the virtual environment with `source ENV/bin/activate`.  (You can type `deactivate` to exit the virtual environment.)
5. Navigate to the backend git repository, and run `pip install -r requirements.txt` to install Python package dependencies.
6. Make sure **onto_lib for python3** is in the python path, and all OBO ontology files required by onto_lib are present.  (For now, the easiest way to accomplish this is to copy onto_lib and the OBO files into /build-db-script/onto_lib .  You might have to edit the onto_lib python files to tell it where to look for the OBO files.)



## Build Mongo database

Build the Mongo database used by the MetaSRA API, using the output of the MetaSRA pipeline.

1. Collect the following files, and update the config variables at the top of build-db-script/build-db.py to specify their locations.
  + MetaSRA database : SQLite file : MetaSRA pipeline output
  + SRA metadata subset DB : SQLite file : a byproduct of the MetaSRA pipeline
  + Recount2 ID list : CSV file : To get this file, go to the Recount2 website and click "Download list of studies matching search results" without applying any filters.
2. The build-db.py script will connect to the Mongo server on localhost using the default port, and create a new database called "metaSRA".  If you need it to do something different, change the new_output_db() function.
3. Activate the virtual environment: navigate to the directory containing "ENV" (your project directory), and run `source ENV/bin/activate`.
4. Navigate to /build-db-script and run `python build-db`


### Copy the MetaSRA Mongo database to another machine

I've found it most convenient to build the database on my development machine, and then copy it to the web server instead of building it straight on the webserver.

1. On the source machine, dump the database into a folder with `mongodump --db=metaSRA --out=metaSRAdumpYYYYMMDD`
2. Copy the folder metaSRAdumpYYYYMMDD to the web server or other target machine
3. If there is a previous version of the metaSRA database on the target machine rename it to metaSRA_old to back it up:
  1. On the target machine, open a Mongo shell, connected to the default database on localhost by calling `mongo`,
  2. If there is already a database called metaSRA_old, drop it with `use metaSRA_old` then `db.dropDatabase()`.
  3. Rename the database with `db.copyDatabase('metaSRA', 'metaSRA_old')`, then `use metaSRA` and `db.dropDatabase()`.  (Mongo doesn't have command to rename a database, you have to copy it and drop the old one.)
  4. Type `exit` to exit the mongo shell and return to the terminal.
4. From the target machine, CD to the parent directory of the database dump and run `mongorestore metaSRAdumpYYYYMMDD` in the terminal to restore the dump onto the Mongo server.



## Run development server

Once everything is set up, change to your MetaSRA project folder (which contains ENV and metasra-backend) and run the following in a terminal:

```bash
# Activate the virtual environment
source ENV/bin/activate

# Change to the folder containing metasra_api.py
cd metasra-backend/src

# Run the Flask development server
export FLASK_APP=metasra_api.py
export FLASK_DEBUG=1
flask run
```

If your editor isn't set up to automatically compile typescript, you can un-comment the lines towards the bottom of metasra_api.py (under "if DEBUG") which start a typescript compiler and watcher when you start the back-end server.  (You should only do this for development, when DEBUG is true.)

The development server is also set up to serve static files from the front-end.  (Only if DEBUG is true - don't do this for deployment.)



## Update back-end on web server
Once you've pushed updates to this git repository, here's how to update the back-end on the server.  You have to 1) pull the changes from the github repository and 2) restart the UWSGI process that runs the Python app.  SSH into the web server, then:

```bash
cd /var/www/metasra-backend
git pull
cd ..
source ENV/bin/activate
ENV/bin/uwsgi --reload server_pid_file.pid --ini /var/www/uwsgi-conf.ini
```

Sometimes UWSGI doesn't start for some reason, so if the website isn't working and you don't see uwsgi in the process list when you do `ps aux`, try starting UWSGI again with `ENV/bin/uwsgi --ini /var/www/uwsgi-conf.ini`.



## Rebuild the web server
These instructions are for completely rebuilding the web server from scratch.

Install and configure NGINX, Python 3, and Mongo > 3.4.  Follow the instructions above to set up the Mongo database, running on localhost using the default connection parameters (or change them in metasra_api.py.)


#### Copy files
Put all of this in /var/www: (or maybe a different directory depending on your system, but this document will refer to /var/www.)
+ metasra-backend : a clone of this repository
+ metasra-frontend : a clone of the front-end respository
+ static : a folder containing static files for the web application that are excluded from the front-end git repository.  As of November 2017, it looks like this:
```
$ ls static
congruent_pentagon.png  footer_lodyas.png  metasra.v1.0.sqlite  metasra_versions      term_ancestors.sqlite  v1.1
email.js                metasra.sqlite     metasra.v1.1.sqlite  publication_datasets  term_names.sqlite
```

+ uwsgi-conf.ini : text file with these contents:
```ini
[uwsgi]
socket = 127.0.0.1:9001
wsgi-file = /var/www/metasra-backend/src/metasra_api.py
pidfile = /var/www/server_pid_file.pid
daemonize = /var/www/uwsgi.log
virtualenv = /var/www/ENV
callable = app
```


#### Install dependencies
Create a new virtual environment, and within it install the dependencies listed in requirements.txt as well as UWSGI.

To install UWSGI from a Python package, the python header/development files must be installed on the system.  (Look for a system package called python3-dev or something similar?)

```bash
cd /var/www
python3 -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
pip install uwsgi
```


#### Configure NGINX routes
Add this to your /etc/nginx/nginx.conf (This file might be in a different location depending on your system.)

```nginx
server {
  server_name  metasra.biostat.wisc.edu;

  # Load configuration files for the default server block.
  include /etc/nginx/default.d/*.conf;

  listen       80;


  location /api/v01 {
     include   uwsgi_params;
     uwsgi_pass  127.0.0.1:9001;
  }

  location /node_modules/ {
     alias /var/www/metasra-frontend/node_modules/;
  }

  location / {
     alias /var/www/metasra-frontend/src/;
     try_files /supportpages/$uri $uri $uri/ /index.html;
  }

  location /static/ {
      root /var/www;
      if (-f $request_filename) {
         rewrite ^/static/(.*)$  /static/$1 break;
      }
  }
}     
```


#### Start UWSGI
This will start WSGI, to serve the back-end.  You should set up the server to do this automatically on startup.

```bash
cd /var/www
source ENV/bin/activate
ENV/bin/uwsgi --ini /var/www/uwsgi-conf.ini
```


#### Compile front-end
See README in front-end repository.
