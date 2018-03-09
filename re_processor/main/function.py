#!/usr/bin/env python
# coding=utf-8
from re_processor.sender import MainSender
import json, operator, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, get_redis, get_redis_la
from datetime import datetime
from re_processor.common import (
    _log, update_virtual_device_log,
    get_sequence, RedisLock, logger,
    get_pks_limit_cache, check_rule_limit,
    new_virtual_device_log,
    getset_rule_last_data,
    set_interval_lock,
    update_virtual_device_log)
import re

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


srv_session = requests.Session()


def calc_logic(func_task, dp_kv, task_vars, msg):
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
    pattern = {
        'number': re.compile(r'^[0-9]+(\.[0-9]+)*$'),
        'string': re.compile(r'^(\'.+\'|".+")$'),
        'hex': re.compile(r'^0(x|X)[0-9a-fA-F]+$')
    }
    def _calculate(params_stack, symbol):
        if cal_opt.has_key(symbol):
            left, right = params_stack[-2:]
            params_stack = params_stack[0:-2]
            params_stack.append(cal_opt[symbol](left, right))
        else:
            params_stack.append(symbol)
        return params_stack
    # print 'func_task:', func_task
    func = func_task.get('content')

    if func_task.get('type') == 'calculator':
        alias = func.get('alias')
        expression = func.get('expression')
        exp = []
        query_list = []
        for symbol in expression:
            if symbol in cal_opt:
                exp.append(symbol)
            elif symbol.split('.')[0] in 'data':
                symbol = dp_kv.get(symbol.replace("data.", ""), 0)
                exp.append(symbol)
            elif pattern['number'].search(symbol):
                exp.append(json.loads(symbol))
            elif symbol.split('.')[0] in ['data', 'common', 'display']:
                query_list.append(symbol)
            else:
                return []
        try:
            res = reduce(_calculate, exp, [0])
        except ZeroDivisionError:
            res = [0]
        dp_kv[alias] = res[1]
        task_vars[alias] = res[1]
        result = True

    if func_task.get('type') == 'script':
        content = func_task.get('content')
        alias = content.get('alias')
        params = func_task.get('params')
        content_vars = {}
        for param in params:
            data = msg.get('data', {})
            value = param.replace("data.", "")
            content_vars.update({param: data.get(value)})
        script_info = run(content['lang'],  content['src_code'], content_vars)
        task_vars.update({alias: script_info})
        result = True
    if func_task.get('type') == 'custom_json':
        custom_info = custom_json(inp)
        task_vars.update(custom_info)
        result = True

    operation = opt.get(func.get('opt'))
    if operation:
        cond1 = func['cond1'].replace("data.", "")
        cond2 = func['cond2']
        if 'data' in cond2:
            cond2 = func['cond2'].replace("data.", "")
            cond2 = dp_kv.get(cond2)
        dp_value = dp_kv.get(cond1)
        if dp_value is None:
            return result
        if pattern['hex'].search(cond2):
            dp_value = '0x{}'.format(dp_value)
        if pattern['string'].search(cond2):
            dp_value = "'{}'".format(unicode(dp_value))
            cond2 = unicode(cond2)

        hex_flag = False
        tmp_list = []
        for tmp in [dp_value, cond2]:
            if pattern['number'].search(str(tmp)):
                tmp_list.append(tmp)
            elif pattern['string'].search(tmp):
                tmp_list.append(tmp[1:-1])
            elif pattern['hex'].search(tmp):
                hex_flag = True
                tmp_list.append(tmp)
        if hex_flag:
            tmp_list[0] = int(tmp_list[0], 16)
            tmp_list[1] = int(tmp_list[1], 16)
        if dp_value is not None:
            if isinstance(tmp_list[0], basestring) and isinstance(tmp_list[1], basestring):
                result = operation(tmp_list[0], tmp_list[1])
            else:
                result = operation(float(tmp_list[0]), float(tmp_list[1]))
            # print 'result:', result
            # print "cond1:", cond1, "dp_value:", dp_value, "op:", operation, "cond2:", cond2, "result:", result
    return result


def custom_json(msg):
    content = msg['content']
    # try:
    #     if content['event'] in ['alert', 'fault'] and msg['task_vars'].get(content['attr'], '') != int(content['attr_type']):
    #         return []
    # except ValueError:
    #     return []
    msg['task_vars'] = {}
    msg['task_vars']['sys.timestamp_ms'] = time.time()
    msg['task_vars'][content['alias']] = get_json(content['content'], {}, int(content['refresh']), content['alias'], msg['rule_id'], msg['task_vars']['sys.timestamp_ms'])
    return msg['task_vars']

def get_json(content, params, refresh, name, rule_id, now_ts):
    cache_key = 're_core_{0}_{1}_custom_json'.format(rule_id, name)
    cache = get_redis()
    result = {}

    while True:
        try:
            result = cache.get(cache_key)
            if result:
                result = json.loads(result)
                break
            else:
                with RedisLock(cache_key) as lock:
                    if lock:
                        _content = json.dumps(content)
                        for key, val in params.items():
                            _content = _content.replace('"${'+key+'}"', json.dumps(val))
                        content = json.loads(_content)
                        url = content['url']
                        headers = content.get('headers', {})
                        data = content.get('data', {})
                        method = content.get('method', 'get')
                        if 'get' == method:
                            response = requests.get(url, headers=headers, params=data)
                        elif 'post' == method:
                            response = requests.post(url, data=json.dumps(data), headers=headers)
                        else:
                            raise Exception(u'invalid http method: {}'.format(method))

                        if response.status_code > 199 and response.status_code < 300:
                            try:
                                result = json.loads(response.content)
                            except ValueError:
                                raise Exception(u'error response: status_code - {0}, content: {1}'.format(response.status_code, response.content[:100]))
                        else:
                            raise Exception(u'error response: status_code - {0}, content: {1}'.format(response.status_code, response.content[:100]))
                        p = cache.pipeline()
                        p.set(cache_key, response.content)
                        p.expire(cache_key, refresh + 5)
                        p.execute()
                        break
            time.sleep(1)
        except redis.exceptions.RedisError, e:
            result = {'error_message': 'redis error: {}'.format(str(e))}

    return result


def get_value_from_json(name, json_obj):
    fields = name.split('.')
    _obj = json_obj
    for field in fields[1:]:
        if type(_obj) is dict:
            if field not in _obj:
                break
        elif type(_obj) is list:
            try:
                field = int(field)
            except ValueError:
                break
            if field >= len(_obj):
                break
        else:
            break

        _obj = _obj[field]
    else:
        return _obj
    return ''


def run(lang, src_code, params):
    url = "{0}{1}{2}".format('http://', settings.SCRIPT_HOST, '/run')
    headers = {
        'Authorization': settings.SCRIPT_API_TOKEN
    }
    data = {
        'lang': lang,
        'src_code': src_code,
        'context': params
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    if response.status_code > 199 and response.status_code < 300:
        return response.json()['result']
    elif 400 == response.status_code:
        raise Exception(u'script error: {}'.format(response.json()['detail_message']))
    else:
        raise Exception(u'script error: {}'.format(response.content))


def generate_msg_func_list(rule, msg, last_data):
    """
    生成d3任务列表(包括function与ouput), 输入列表, 输出id列表
    """
    input_list = []
    task_obj = {}
    rule_tree = rule.get('rule_tree', {})
    event = rule_tree.get('event', {})
    event_input = event.get('data', [])
    changed_input = event.get('change', [])
    output_wires = []
    custom_vars = rule.get('custom_vars', {})
    if custom_vars:
        for _, custom in enumerate(custom_vars):
            custom_vars[custom]['rule_id'] = rule.get('rule_id')
            input_list.append(custom_vars[custom])

    if changed_input:
        data = msg.get('data', {})
        for inp in changed_input:
            if inp['category'] == 'input':
                last_data = getset_rule_last_data(data, rule['rule_id'], msg['did'])
                print 'last data:', last_data
                change_events = filter(
                    lambda _node: reduce(lambda res, y: res or \
                                         (data.get(y, None) is not None and \
                                          last_data.get(y, None) != data.get(y, None)),
                                         _node['content'].get('params', []),
                                         False),
                    rule['rule_tree']['event'].get('change', []))
                if change_events:
                    input_list.append(inp)

    if event_input:
        for inp in event_input:
            if inp['category'] == 'input':
                # if 'virtual:site' == msg['mac']:
                #     log_id = new_virtual_device_log(msg['product_key'], rule.get('rule_id'))
                input_list.append(inp)
    task_list = rule_tree['task_list']
    for task in task_list:
        t = task_list[task]
        if t['category'] == 'output':
			output_wires.append(task)
        task_obj.update({task: t})
    return (task_obj, input_list, output_wires)


def next(task_obj, func_task):
    data_list = []
    if func_task['wires']:
        data = map(lambda y: y,func_task['wires'][0])
        for d in data:
            info = task_obj.get(d)
            data_list.append(info)
    return data_list


def generate_func_list_msg(task_obj, input_wires_id, dp_kv, output_wires, task_vars, log_id, msg):
    func_arr = []
    func_task = task_obj.get(input_wires_id)
    output_obj = {}
    if func_task['category'] == 'output':
        output_obj.update({func_task['id']: func_task['id']})
    result = calc_logic(func_task, dp_kv, task_vars, msg)
    if not result and msg['mac'] == 'virtual:site':
        update_virtual_device_log(log_id, 'triggle', 2, '')
    func_data = [func_task]
    wires_list = []
    while result:
        while len(func_data) > 1:
            tasks = []
            for func_task in func_data:
                result = calc_logic(func_task, dp_kv, task_vars, msg)
                # print 'func task', func_task
                # print 'result:', result
                if result:
                    func_data =  next(task_obj, func_task)
                    tasks.extend(func_data)
                    for wire in func_task['wires'][0]:
                        wires_list.append(wire)
            func_data = tasks
        else:
            for func_task in func_data:
                result = calc_logic(func_task, dp_kv, task_vars, msg)
                if result:
                    func_data =  next(task_obj, func_task)
                if result:
                    for wire in func_task['wires'][0]:
                        wires_list.append(wire)
                if func_task['category'] == 'output':
                    func_data = []
            # print 'func task', func_task
            # print 'result:', result
            # print 'func data:', func_data
    #             print 'func data:', func_data
    # print 'wires list:', wires_list
    # print 'output wires:', output_wires
    for wires in wires_list:
        if wires in output_wires:
            output_obj.update({wires: wires})
    # print 'output:', output_obj
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

def send_output_msg(output, msg, log, vars_info, log_id, rule_id, p_log):
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
    task_vars['common.rule_id'] = rule_id
    task_vars['product_key'] = msg['product_key']
    task_vars['common.product_key'] = msg['product_key']
    msg_data = msg.get('data', {})
    for k,v in enumerate(msg_data):
        task_vars['data.{}'.format(v)] = msg_data[v]
    for _,value in enumerate(vars_info):
        task_vars[value] = vars_info[value]
    sender = MainSender(product_key)
    content = output.get('content')
    en_tpl = content.get("english_template")
    tpl = content.get("template")
    params_list = output.get('params')
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
    if msg['mac'] == 'virtual:site':
        update_virtual_device_log(log_id, 'triggle', 1, '')
    if 'alias' in extern_params:
        alias.update(extern_alias(task_vars, content))
        product_info  = _query_product_name(task_vars)
        for values in alias.values():
            emp = filter(lambda x: not x.get('dev_alias', ''), values)
            if emp:
                for v in values:
                    if not v.get('dev_alias', ''):
                        v['dev_alias'] = product_info.get('common.product_name')

    message = {
        "product_key": product_key,
        "did": msg['did'],
        "mac": msg['mac'],
        "params": params_obj,
        "action_type": output['type'],
        "extern_params": {
            "alias": alias
        },
        "log_data": {
            "log_id": log_id,
            "field": "action",
            "value": output['type']
        },
        "content": json.dumps(content)
    }
    limit_dict = get_pks_limit_cache()
    limit_info = limit_dict.get(product_key, {})
    triggle_limit = limit_info.get('triggle_limit')
    if check_rule_limit(product_key, triggle_limit, 'triggle'):
        sender.send(message, product_key)
    else:
        log['error_message'] = 'quota was used up'
