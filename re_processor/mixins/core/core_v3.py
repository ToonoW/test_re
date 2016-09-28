#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, redis_pool
from re_processor.common import _log

from core_v1 import get_value_from_json


class BaseCore(object):
    """BaseCore"""

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list, log_flag)
        '''
        msg_list = getattr(self, msg['current']['type'])(msg)

        return (True if msg_list else False), msg_list, (False if msg_list else True)

    def next(self, msg):
        return reduce(operator.__add__,
                      map(lambda y: map(lambda x: dict(copy.deepcopy(msg), current=msg['task_list'][x]),
                                        msg['current']['wires'][y]),
                          msg['current']['ports']),
                      [])


class InputCore(BaseCore):
    """InputCore"""
    core_name = 'input'

    def device_data(self, msg):
        content = msg['current']['content']
        try:
            if content['event'] in ['alert', 'fault'] and msg['task_vars'].get(content['attr'], '') != int(content['attr_type']):
                return []
        except ValueError:
            return []
        params = msg['current'].get('params', [])
        if params:
            query_result = self._query(msg['task_vars'], params)
            msg['task_vars'].update(query_result)
            not_found = filter(lambda x: not query_result.has_key(x), params)
            if not_found:
                msg['task_vars'].update({x: '' for x in not_found})

        return self.next(msg)

    def _query(self, task_vars, params_list):
        result = {}
        prefix = list(set([x.split('.')[0] for x in params_list]))

        if 'data' in prefix or 'common.location' in params_list:
            result.update(self._query_data(task_vars))

        if 'display' in prefix:
            result.update(self._query_display(task_vars))

        if 'common.product_name' in params_list:
            result.update(self._query_product_name(task_vars))

        return result

    def _query_data(self, task_vars):
        db = get_mongodb('data')
        ds = db['device_status']

        try:
            status = ds.find_one({'did': task_vars['did']})
            result = {'.'.join(['data', k]): v for k, v in status['attr']['0'].items()}
            result['online.status'] = 1 if status['is_online'] else 0
            result['offline.status'] = 0 if status['is_online'] else 1
            result['common.location'] = status.get('city', 'guangzhou')
        except KeyError:
            result = {}

        return result

    def _query_display(self, task_vars):
        db = get_mongodb('core')
        ds = db['datapoints']
        ds_extra = db['parsers']

        return_result = {}

        try:
            status = ds.find_one({'product_key': task_vars['product_key']})
            if status:
                result = {'.'.join(['display', x['name']]): x['display_name'] for x in status['datas']['entities'][0]['attrs']}
                result['common.product_name'] = status['datas']['name']
            else:
                result = {}
        except KeyError:
            result = {}

        return_result.update(result)

        try:
            status = ds_extra.find_one({'product_key': task_vars['product_key']})
            if status:
                result = {'.'.join(['display', x['name']]): x['display_name'] for x in status['ext_data_points']}
            else:
                result = {}
        except KeyError:
            result = {}

        return_result.update(result)

        return return_result

    def _query_product_name(self, task_vars):
        url = "{0}{1}{2}{3}".format('http://', settings.HOST_GET_BINDING, '/v1/products/', task_vars['product_key'])
        headers = {
            'Authorization': settings.INNER_API_TOKEN
        }
        result = {}
        try:
            response = requests.get(url, headers=headers)
            data = json.loads(response.content)
            result['common.product_name'] = data['name']
        except:
            pass

        return result

    def custom_json(self, msg):
        content = msg['current']['content']
        try:
            if content['event'] in ['alert', 'fault'] and msg['task_vars'].get(content['attr'], '') != int(content['attr_type']):
                return []
        except ValueError:
            return []
        msg['task_vars'][content['alias']] = self.get_json(content['content'], {}, int(content['refresh']), content['alias'], msg['rule_id'], msg['task_vars']['sys.timestamp_ms'])

        return self.next(msg)

    def get_json(self, content, params, refresh, name, rule_id, now_ts):
        cache_key = 're_core_{0}_{1}'.format(rule_id, name)
        cache = redis.Redis(connection_pool=redis_pool)
        result = {}
        do_refresh = True
        try:
            if cache.get(cache_key + '_lock'):
                do_refresh = False
            else:
                is_success = cache.setnx(cache_key + '_lock', json.dumps(now_ts))
                if is_success:
                    cache.setex(cache_key + '_lock', json.dumps(now_ts), refresh)
                    do_refresh = True
                else:
                    do_refresh = False
        except:
            pass

        if do_refresh:
            _content = json.dumps(content)
            for key, val in params.items():
                _content = _content.replace('"${'+key+'}"', json.dumps(val))
            content = json.loads(_content)
            url = content['url']
            headers = content.get('headers', {})
            data = content.get('data', {})
            method = content.get('method', 'get')
            if 'get' == method:
                query_string = '&'.join(map(lambda x: '{0}={1}'.format(*x), data.items())) if data else ''
                url = (url + '?' + query_string) if query_string else url
                response = requests.get(url, headers=headers)
            elif 'post' == method:
                response = requests.post(url, data=json.dumps(data), headers=headers)
            else:
                raise Exception('invalid method: {}'.format(method))

            result = json.loads(response.content)

            try:
                cache.set(cache_key, response.content)
            except:
                pass
        else:
            try:
                result = cache.get(cache_key)
                result = json.loads(result)
            except:
                pass

        return result


class FuncCore(BaseCore):
    """FuncCore"""
    core_name = 'function'

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

    def condition(self, msg):
        content = msg['current']['content']
        query_list = []
        hex_flag = False
        tmp_list = []
        for tmp in [content['cond1'], content['cond2']]:
            if tmp in msg['task_vars']:
                tmp_list.append(msg['task_vars'][tmp])
            elif self.pattern['number'].search(tmp):
                tmp_list.append(json.loads(tmp))
            elif self.pattern['string'].search(tmp):
                tmp_list.append(tmp[1:-1])
            elif self.pattern['hex'].search(tmp):
                hex_flag = True
            elif tmp.split('.')[0] in msg['custom_vars']:
                alias = tmp.split('.')[0]
                if alias in msg['task_vars']:
                    tmp = get_value_from_json(tmp, msg['task_vars'][alias])
                    tmp_list.append(tmp)
                else:
                    next_node = copy.deepcopy(msg['custom_vars'][alias])
                    next_node['ports'] = [0]
                    next_node['wires'] = [[msg['current']['id']]]
                    return [dict(copy.deepcopy(msg), current=next_node)]
            elif tmp.split('.')[0] in ['data', 'common', 'display']:
                query_list.append(tmp)
            else:
                return []

        if query_list:
            next_node = {
                "id": "uuid",
                "params": query_list,
                "category": "input",
                "type": "device_data",
                "inputs": 0,
                "outputs": 1,
                "ports": [0],
                "wires": [
                    [msg['current']['id']]
                ],
                "content": {
                    "event": "data",
                    "data_type": "device_data",
                    "refresh": 3600,
                    "interval": 10
                }
            }
            return [dict(copy.deepcopy(msg), current=next_node)]

        if hex_flag:
            tmp_list[0] = int(tmp_list[0], 16)
            tmp_list[1] = int(tmp_list[1], 16)

        result = self.opt[content['opt']](tmp_list[0], tmp_list[1])

        return self.next(msg) if result else []

    def calculator(self, msg):
        content = msg['current']['content']
        exp = []
        query_list = []

        for symbol in content['expression']:
            if symbol in self.cal_opt:
                exp.append(symbol)
            elif symbol in msg['task_vars']:
                exp.append(float(msg['task_vars'][symbol]))
            elif self.pattern['number'].search(symbol):
                exp.append(json.loads(symbol))
            elif symbol.split('.')[0] in msg['custom_vars']:
                alias = symbol.split('.')[0]
                if alias in msg['task_vars']:
                    exp.append(get_value_from_json(symbol, msg['task_vars'][alias]))
                else:
                    next_node = copy.deepcopy(msg['custom_vars'][alias])
                    next_node['ports'] = [0]
                    next_node['wires'] = [[msg['current']['id']]]
                    return [dict(copy.deepcopy(msg), current=next_node)]

            elif symbol.split('.')[0] in ['data', 'common', 'display']:
                query_list.append(symbol)
            else:
                return []

        if query_list:
            next_node = {
                "id": "uuid",
                "params": query_list,
                "category": "input",
                "type": "device_data",
                "inputs": 0,
                "outputs": 1,
                "ports": [0],
                "wires": [
                    [msg['current']['id']]
                ],
                "content": {
                    "event": "data",
                    "data_type": "device_data",
                    "refresh": 3600,
                    "interval": 10
                }
            }
            return [dict(copy.deepcopy(msg), current=next_node)]

        try:
            res = reduce(self._calculate, exp, [0])
        except ZeroDivisionError:
            res = [0]
        msg['task_vars'][content['alias']] = res.pop()

        return self.next(msg)

    def _calculate(self, params_stack, symbol):
        if self.cal_opt.has_key(symbol):
            left, right = params_stack[-2:]
            params_stack = params_stack[0:-2]
            params_stack.append(self.cal_opt[symbol](left, right))
        else:
            params_stack.append(symbol)

        return params_stack

    def script(self, msg):
        content = msg['current']['content']
        params = {}
        query_list = []

        for symbol in msg['current']['params']:
            if symbol in msg['task_vars']:
                params[symbol] = msg['task_vars'][symbol]
            elif symbol.split('.')[0] in msg['custom_vars']:
                alias = symbol.split('.')[0]
                if alias in msg['task_vars']:
                    params[symbol] = get_value_from_json(symbol, msg['task_vars'][alias])
                else:
                    next_node = copy.deepcopy(msg['custom_vars'][alias])
                    next_node['ports'] = [0]
                    next_node['wires'] = [[msg['current']['id']]]
                    return [dict(copy.deepcopy(msg), current=next_node)]
            elif symbol.split('.')[0] in ['data', 'common', 'display']:
                query_list.append(symbol)
            else:
                return []

        if query_list:
            next_node = {
                "id": "uuid",
                "params": query_list,
                "category": "input",
                "type": "device_data",
                "inputs": 0,
                "outputs": 1,
                "ports": [0],
                "wires": [
                    [msg['current']['id']]
                ],
                "content": {
                    "event": "data",
                    "data_type": "device_data",
                    "refresh": 3600,
                    "interval": 10
                }
            }
            return [dict(copy.deepcopy(msg), current=next_node)]

        msg['task_vars'][content['alias']] = self.run(content['script_id'], params)

        return self.next(msg)

    def run(self, script_id, params):
        url = "{0}{1}{2}".format('http://', settings.SCRIPT_HOST, '/run')
        headers = {
            'Authorization': settings.SCRIPT_API_TOKEN
        }
        data = {
            'script_id': script_id,
            'context': params
        }
        response = requests.post(url, data=json.dumps(data), headers=headers)
        return json.loads(response.content)['result']


class OutputCore(BaseCore):
    """OutputCore"""
    core_name = 'output'

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list, log_flag)
        '''
        msg_list, log_flag = self.output(msg)

        return (True if msg_list else False), msg_list, log_flag

    def output(self, msg):
        content = msg['current']['content']

        if msg['current']['allow_time']:
            time_now = map(int, time.strftime('%m-%d-%H-%w').split('-'))
            time_month, time_day, time_hour, time_week = msg['current']['allow_time'].get('month', []), msg['current']['allow_time'].get('day', []), msg['current']['allow_time'].get('hour', []), msg['current']['allow_time'].get('week', [])
            if (time_month and time_now[0] not in time_month) or \
                    (time_day and time_now[1] not in time_day) or \
                    (time_hour and time_now[2] not in time_hour) or \
                    (time_week and time_now[3] % 7 not in time_week):
                p_log = {
                    'module': 're_processor',
                    'rule_id': msg.get('rule_id', ''),
                    'id': msg['current']['id'],
                    'event': msg.get('event', ''),
                    'product_key': msg['task_vars'].get('product_key', ''),
                    'did': msg['task_vars'].get('did', ''),
                    'mac': msg['task_vars'].get('mac', ''),
                    'time_now': 'month: {0}, day: {1}, hour: {2}, week: {3}'.format(*time_now),
                    'result': 'failed',
                    'handling': 'action',
                    'error_message': 'time now is not in list of allow_time'
                }
                _log(p_log)
                return [], True

        extern_list = []
        if 'extern_params' in msg['current']:
            extern_list = [x for x in msg['current']['extern_params'] if x not in msg['extern_params']]

        if extern_list:
            extern_result = self._query_extern(msg['task_vars'], extern_list, content)
            msg['extern_params'].update(extern_result)

        params = {}
        query_list = []
        for symbol in msg['current']['params']:
            if symbol in msg['task_vars']:
                params[symbol] = msg['task_vars'][symbol]
            elif symbol.split('.')[0] in msg['custom_vars']:
                alias = symbol.split('.')[0]
                if alias in msg['task_vars']:
                    params[symbol] = get_value_from_json(symbol, msg['task_vars'][alias])
                else:
                    next_node = copy.deepcopy(msg['custom_vars'][alias])
                    next_node['ports'] = [0]
                    next_node['wires'] = [[msg['current']['id']]]
                    return [dict(copy.deepcopy(msg), current=next_node)], False
            elif symbol.split('.')[0] in ['data', 'common', 'display']:
                query_list.append(symbol)
            else:
                params[symbol] = ''

        if query_list:
            next_node = {
                "id": "uuid",
                "params": query_list,
                "category": "input",
                "type": "device_data",
                "inputs": 0,
                "outputs": 1,
                "ports": [0],
                "wires": [
                    [msg['current']['id']]
                ],
                "content": {
                    "event": "data",
                    "data_type": "device_data",
                    "refresh": 3600,
                    "interval": 10
                }
            }
            return [dict(copy.deepcopy(msg), current=next_node)], False

        _msg = {
            'msg_to': settings.MSG_TO['external'],
            'action_type': msg['current']['type'],
            'event': msg.get('event', ''),
            'rule_id': msg.get('rule_id', ''),
            'product_key': msg['task_vars'].get('product_key', ''),
            'did': msg['task_vars'].get('did', ''),
            'mac': msg['task_vars'].get('mac', ''),
            'ts': time.time(),
            'params': params,
            'extern_params': msg['extern_params'],
            'content': json.dumps(content)
        }

        return [_msg], True

    def _query_extern(self, task_vars, extern_list, content):
        extern_list = list(set(extern_list))
        result = {x: getattr(self, 'extern_' + x)(task_vars, content) for x in extern_list}
        return result

    def extern_alias(self, task_vars, content):
        url = "{0}{1}{2}{3}".format('http://', settings.HOST_GET_BINDING, '/v1/bindings/', task_vars['did'])
        headers = {
            'Authorization': settings.INNER_API_TOKEN
        }
        try:
            app_id = content.get('app_id', '')
            if app_id:
                response = requests.get(url, headers=headers)
                resp_data = json.loads(response.content)
                data = {app_id: resp_data[app_id]}
            else:
                data = {}

        except:
            data = {}

        return data
