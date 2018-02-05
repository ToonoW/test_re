#!/usr/bin/env python
# coding=utf-8
from re_processor.sender import MainSender
import json, operator, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, get_redis, get_redis_la
from datetime import datetime
from re_processor.common import _log, update_virtual_device_log, get_sequence, RedisLock, logger


srv_session = requests.Session()


def calc_logic(func_task, dp_kv):
	result = False
	opt = {
		'>': operator.__gt__,
		'<': operator.__lt__,
		'==': operator.__eq__,
		'>=': operator.__ge__,
		'<=': operator.__le__,
		'!=': operator.__ne__
	}
	cal_opt = {
		'+': operator.__add__,
		'-': operator.__sub__,
		'*': operator.__mul__,
		'/': operator.__div__
	}
	func = func_task.get('content')
	operation = opt.get(func.get('opt'))
	if operation:
		cond1 = func['cond1'].replace("data.", "")
		cond2 = func['cond2']
		dp_value = dp_kv.get(cond1)
		if dp_value is not None:
			result = operation(dp_value, float(cond2))
			# print "cond1:", cond1, "dp_value:", dp_value, "op:", operation, "cond2:", cond2, "result:", result
	return result

def generate_func_list_msg(task_obj, input_wires_id, dp_kv, output_wires):
	func_arr = []
	func_task = task_obj.get(input_wires_id)
	result = calc_logic(func_task, dp_kv)
	output_obj = {}
	while result and func_task['category'] != 'output':
		if func_task['wires']:
			for wire in func_task['wires'][0]:
				task = task_obj.get(wire)
				if not task['wires']:
					output_obj.update({wire: wire})
				result = calc_logic(func_task, dp_kv)
				if not result:
					wires_info = func_task['wires']
					if wires_info:
						for wire in wires_info[0]:
							if output_obj.get(wire):
								del output_obj[wire]
				if task['wires']:
					for tw in task['wires'][0]:
						result = calc_logic(task, dp_kv)
						if result and tw in output_wires:
							output_obj.update({tw: tw})
				func_task = task_obj.get(wire)
	return output_obj


def query(task_vars, params_list):
	result = {}
	prefix = list(set([x.split('.')[0] for x in params_list]))
	if 'data' in prefix:
		result.update(_query_data(task_vars))
	if 'common.location' in params_list:
		result.update(_query_location(task_vars))
	if 'display' in prefix:
		try:
			display_data = _query_display(task_vars)
		except Exception, e:
			print 'exception:', str(e)
		if not display_data:
			display_data = {}
		result.update(display_data)
	if 'common.product_name' in params_list:
		result.update(_query_product_name(task_vars))

	return result


def extern_alias(task_vars, content):
	app_id = content.get('app_id', '')
	uids = content.get('uids', [])
	if not app_id:
		return {}
	url = "http://{0}/v1/bindings/{1}?appids={2}".format(settings.HOST_GET_BINDING, task_vars['did'], app_id)
	headers = {
		'Authorization': settings.INNER_API_TOKEN
	}
	try:
		response = srv_session.get(url, headers=headers)
		data = json.loads(response.content)
	except:
		data = {}

	return {k: filter(lambda x: x['uid'] in uids, v) for k, v in data.items()} if uids else data


def _query_display(task_vars):
	db = get_mongodb('core')
	ds = db['datapoints']

	return_result = {}

	try:
		status = ds.find_one({'product_key': task_vars['product_key']})
		if status:
			result = {'.'.join(['display', x['name']]): x['display_name'] for x in status['datas']['entities'][0]['attrs']}
		else:
			result = {}
	except KeyError:
		result = {}
	return result


def _query_location(task_vars):
	db = get_mongodb('data')
	ds = db['device_status']
	result = {}

	try:
		status = ds.find_one({'did': task_vars['did']})
		result['common.location'] = status.get('city', 'guangzhou')
	except Exception, e:
		logger.exception(e)

	return result


def _query_data(task_vars):
	result = {}

	try:
		cache_la = get_redis_la()
		data = cache_la.get('dev_latest:{}'.format(task_vars['did']))
		if data:
			data = json.loads(data)
			result.update({'.'.join(['data', k]): v for k, v in data['attr'].items()})
	except Exception, e:
		logger.exception(e)

	return result


def _query_product_name(task_vars):
	url = "{0}{1}{2}{3}".format('http://', settings.HOST_GET_BINDING, '/v1/products/', task_vars['product_key'])
	headers = {
		'Authorization': settings.INNER_API_TOKEN
	}
	result = {}
	try:
		response = srv_session.get(url, headers=headers)
		data = json.loads(response.content)
		result['common.product_name'] = data['name']
	except Exception, e:
		logger.exception(e)

	return result

def send_output_msg(output, msg, log):
	product_key = msg['product_key']
	task_vars = {}
	task_vars['sys.timestamp_ms'] = int(log['ts'] * 1000)
	task_vars['sys.timestamp'] = int(log['ts'])
	task_vars['sys.time_now'] = time.strftime('%Y-%m-%d %a %H:%M:%S')
	task_vars['sys.utc_now'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
	task_vars['common.did'] = msg['did']
	task_vars['did'] = msg['did']
	task_vars['common.mac'] = msg['mac'].lower()
	task_vars['common.mac_upper'] = msg['mac'].upper()
	task_vars['product_key'] = msg['product_key']
	task_vars['common.product_key'] = msg['product_key']
	msg_data = msg.get('data', {})
	for k,v in enumerate(msg_data):
		task_vars['data.{}'.format(v)] = msg_data[v]
	sender = MainSender(product_key)
	content = output.get('content')
	en_tpl = content.get("english_template")
	tpl = content.get("template")
	params_list = output.get('params')
	# print 'params list:', params_list
	# print 'task_vars:', task_vars
	params_result = query(task_vars, params_list)
	task_vars.update(params_result)
	params_obj = {}
	for param in params_list:
		if task_vars.get(param) is not None:
			params_obj.update({
				param: task_vars.get(param)
			})
	extern_params = output.get('extern_params', {})
	alias = {}
	if 'alias' in extern_params:
		alias.update(extern_alias(task_vars, content))
	message = {
		"product_key": product_key,
		"did": msg['did'],
		"mac": msg['mac'],
		"params": params_obj,
		"action_type": output['type'],
		"extern_params": {
			"alias": alias
		},
		"content": json.dumps({
			"app_id": content.get('app_id'),
			"title": content.get('title', '通知'),
			"ptype": content.get('ptype'),
			"english_template": en_tpl,
			"template": tpl
		})
	}
	sender.send(message, product_key)
