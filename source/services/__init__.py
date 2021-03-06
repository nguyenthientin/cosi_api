from services.context.worker import run as context_search
from services.id.worker import run as retrieve_by_key
from services.parse_input_string.worker import run as input_string_parser
from services.context.worker import info as context_search_info
from services.id.worker import info as retrieve_by_key_info

max_retrieve_pattern = 10

services = {}
services['context_search'] = context_search
services['data_retrieval'] = retrieve_by_key
services['parse_input'] = input_string_parser

info = {}
info['context_search'] = context_search_info
info['data_retrieval'] = retrieve_by_key_info

db_valid_fields = ['speed', 'flow']
