{% macro create_udf_parse_python_obj() %}

CREATE OR REPLACE FUNCTION {{ target.schema }}.parse_python_obj(text_input STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'parse_it'
AS
$$
import ast
import json

def parse_it(text_input):
    if not text_input:
        return None
    try:
        # Safely evaluates the string as a Python literal (Dict/List)
        parsed = ast.literal_eval(text_input)
        # If it's a list (as seen in Samsung PIM data), take the first element
        if isinstance(parsed, list) and len(parsed) > 0:
            return parsed[0] 
        return parsed
    except:
        return None
$$;

{% endmacro %}
