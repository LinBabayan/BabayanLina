import sys
import os
from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*", "allow_headers": ["Content-Type", "Authorization"]}})

# Add the root directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any
import json

from DBaccess.CrossMatching import CrossMatching
from DBaccess.DBAccessGaia import DBAccessGaia
from DBaccess.DBAccessSDSS import DBAccessSDSS
from DBaccess.DBAccessSimbad import DBAccessSimbad
from DBaccess.DBAccessStellaris import DBAccessStellaris
from DBaccess.RequestProcessingError import RequestProcessingError
from DBaccess.RequestProcessingError import CrossMatchRequestProcessingError

DB_CLASSES = {
    "gaia": DBAccessGaia(),
    "sdss": DBAccessSDSS(),
    "stellaris": DBAccessStellaris(),
    "simbad": DBAccessSimbad()
}


@app.route('/processQuery', methods=['POST', 'OPTIONS'])
def process_query():
    if request.method == 'OPTIONS':
        # Preflight request handler
        response = app.make_default_options_response()
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response
    try:
        db_access, db_name, query_params_json, limit, chunk_size = __validateProcessQueryInput(request.data)

        # Call QueryCatalog
        catalog, query, columns, data, table = db_access.QueryCatalog(query_params_json, limit, chunk_size)
        return __form_success_response(catalog, query, columns, data), 200
    
    except ValueError as e:
        return __form_error_response("","", "Input error", str(e)), 400
    except RequestProcessingError as e:
        return __form_error_response(e.catalog, e.query, e.status, str(e)), 401
    except Exception as e:
        return __form_error_response("","","System error", str(e)), 500

@app.route('/crossMatching', methods=['POST'])
def cross_matching():
    try:
        db_access_src, db_name_src, db_access_to_match, db_name_to_match, query_params_json, limit, chunk_size = __validateCrossMatchingInput(request.data)

        catalog_source, query, catalog_to_match, crossmatch_query, columns, data = CrossMatching().CrossMatching(db_access_src, db_access_to_match, query_params_json, limit, chunk_size)
        return __form_success_crossmatch_response(catalog_source, query, catalog_to_match, crossmatch_query, columns, data), 200

    except ValueError as e:
        return __form_error_crossmatch_response("","", "","", "Input error", str(e)), 400
    except CrossMatchRequestProcessingError as e:
        return __form_error_crossmatch_response(e.catalog_source, e.query, e.catalog_to_match, e.crossmatch_query, e.status, str(e)), 401
    except Exception as e:
        return __form_error_crossmatch_response("","", "", "", "System error", str(e)), 500

#validate input data for process_query
def __validateProcessQueryInput(request_data) -> tuple[DB_CLASSES, Any, Any, int, int]: #protected(_)
    # convert JSON input string to a dictionary
    try:
        query_json = json.loads(request_data)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON for the request: {request_data}")

    # Extract database name
    db_access, db_name = __validateDBName(query_json, 'db_name')

    query_params_json, limit, chunk_size = __validateQueryParams(query_json)

    return db_access, db_name, query_params_json, limit, chunk_size

#validate input data for cross_matching, extract
def __validateCrossMatchingInput(request_data) -> tuple[DB_CLASSES, Any, DB_CLASSES, Any, Any, int, int]: #protected(_)
    # convert JSON input string to a dictionary
    try:
        query_json = json.loads(request_data)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON for the request: {request_data}")

    # Extract database name
    db_access_src, db_name_src = __validateDBName(query_json, 'db_name_src')

    #db_access_to_match, db_name_to_match = query_json.get('db_name_to_match')
    #hardcode matching DB name, in future it will be passed from client side
    db_name_to_match = 'simbad'
    db_access_to_match = DB_CLASSES.get(db_name_to_match)

    query_params_json, limit, chunk_size = __validateQueryParams(query_json)

    return db_access_src, db_name_src, db_access_to_match, db_name_to_match, query_params_json, limit, chunk_size

#validate input string to make sure that it contains db name, extract it and get corresponding DBAccess
def __validateDBName(query_json, db_name_key) -> tuple [DB_CLASSES, Any]:
    db_name_value = query_json.get(db_name_key)
    if not db_name_value:
        raise ValueError(f"Absent {db_name_key} in the request")

    db_access = DB_CLASSES.get(db_name_value.lower())
    if not db_name_value:
        raise ValueError("Not supported DB name: {db_name_value}")
    return db_access, db_name_value

#validate query_params json from input data, load query_params string to json, extract limit, chunk_size
def __validateQueryParams(query_json) -> tuple[Any, int, int]:
    # Extract JSON string of query parameters and convert it to a dictionary
    query_params = query_json.get('query_params')
    if not query_params:
        raise ValueError("Absent 'query_params' in the request: {request_data}")

    try:
        query_params_json = json.loads(query_params)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON for query parameters: {query_params}")

    limit = query_params_json.get('limit')
    if limit:
        try:
            limit = int(limit)
        except Exception:
            raise ValueError(f"Cannot convert to integer limit value: {limit}")

    chunk_size = query_params_json.get('chunkSize')
    if chunk_size:
        try:
            chunk_size = int(chunk_size)
        except Exception:
            raise ValueError(f"annot convert to integer chunkSize value: {chunk_size}")

    if not limit and not chunk_size:
        raise ValueError("Either limit or chunk_size must be specified in query_params")

    return query_params_json, limit, chunk_size

def __form_success_response (catalog, query, columns, data):
    return json.dumps({
        "catalog": catalog,
        "query": query,
        "columns": columns,
        "data": data,
        "status": "success",
        "error": None
    }, cls=NumpyEncoder, indent=2)

def __form_error_response(catalog, query, status, error_message):
    return json.dumps({
        "catalog": catalog,
        "query": query,
        "columns": [],
        "data": [],
        "status": status,
        "error": error_message
    }, indent=2)

def __form_success_crossmatch_response (catalog_source, query, catalog_to_match, crossmatch_query, columns, data):
    return json.dumps({
        "catalog_source": catalog_source,
        "query_source": query,
        "catalog_to_match": catalog_to_match,
        "crossmatch_query": crossmatch_query,
        "columns": columns,
        "data": data,
        "status": "success",
        "error": None
    }, cls=NumpyEncoder, indent=2)

def __form_error_crossmatch_response(catalog_source, query, catalog_to_match, crossmatch_query, status, error_message):
    return json.dumps({
        "catalog_source": catalog_source,
        "query_source": query,
        "catalog_to_match": catalog_to_match,
        "crossmatch_query": crossmatch_query,
        "columns": [],
        "data": [],
        "status": status,
        "error": error_message
    }, indent=2)

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'item'):  # Convert numpy types
            return obj.item()
        return super().default(obj)

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
