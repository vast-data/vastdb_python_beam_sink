# Apache Beam starter for Python with VastDB

If you want to clone this repository to start your own project,
you can choose the license you prefer and feel free to delete anything related to the license you are dropping.

This project is based on [apache/beam-starter-python](https://github.com/apache/beam-starter-python)

## Before you begin

Make sure you have a [Python 3](https://www.python.org/) development environment ready.
If you don't, you can download and install it from the
[Python downloads page](https://www.python.org/downloads/).

We recommend using a virtual environment to isolate your project's dependencies.

```sh
# Create a new Python virtual environment.
python -m venv env

# Activate the virtual environment.
source env/bin/activate
```

While activated, your `python` and `pip` commands will point to the virtual environment,
so any changes or install dependencies are self-contained.

As a one time setup, let's install the project's dependencies from the [`requirements.txt`](requirements.txt) file.

```py
# It's always a good idea to update pip before installing dependencies.
pip install -U pip

# Install the project as a local package, this installs all the dependencies as well.
pip install -e .
```

> ℹ️ Once you are done, you can run the `deactivate` command to go back to your global Python installation.

### Edit the Vast DB source

Currently, this MVP creates two hard-coded records.

The schema is defined in [my_app/app.py](my_app/app.py):

```python
'pa_schema': pa.schema([
    ('id', pa.int64()),
    ('first_name', pa.utf8()),
    ('last_name', pa.utf8())
    ])
```

The records are created within a pipeline, also in [my_app/app.py](my_app/app.py):

```python
| "Create elements" >> beam.Create([
    { 'id': 1, 'first_name': 'John', 'last_name': 'Doe' },
    { 'id': 2, 'first_name': 'Jane', 'last_name': 'Doe' }
    ])
```

### Running the pipeline

Running your pipeline in Python is as easy as running the script file directly.

```sh
# You can run the script file directly.
python main.py \
    --vastdb-endpoint="http://your_endpoint" \
    --vastdb-access-key-id="your_access_key" \
    --vastdb-secret-access-key="your_secret_key" \
    --vastdb-bucket-name="vastdb" \
    --vastdb-schema-name="vastschema" \
    --vastdb-table-name="your_table_name"
```
