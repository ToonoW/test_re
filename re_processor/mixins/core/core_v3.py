#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, get_redis, get_redis_la
from re_processor.common import _log, update_virtual_device_log, get_sequence, RedisLock
from re_processor.data_transform import DataTransformer

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

        if content.get('did', ''):
            alias = content.get('alias', '')
            msg['task_vars'][alias] = self._query_device_data(msg['task_vars'], content['did'])

        return self.next(msg)

    def _query_device_data(self, task_vars, did):
        if did in task_vars:
            return task_vars[did]

        result = {}

        try:
            cache_la = get_redis_la()
            data = cache_la.get('dev_latest:{}'.format(did))
            if data:
                data = json.loads(data)
                result = data['attr']
                task_vars[did] = result
        except redis.exceptions.RedisError:
            pass
        except KeyError:
            pass

        return result

    def _query(self, task_vars, params_list):
        result = {}
        prefix = list(set([x.split('.')[0] for x in params_list]))

        if 'data' in prefix:
            result.update(self._query_data(task_vars))

        if 'common.location' in params_list:
            result.update(self._query_location(task_vars))

        if 'display' in prefix:
            result.update(self._query_display(task_vars))

        if 'common.product_name' in params_list:
            result.update(self._query_product_name(task_vars))

        return result

    def _query_data(self, task_vars):
        result = {}

        try:
            cache_la = get_redis_la()
            data = cache_la.get('dev_latest:{}'.format(task_vars['did']))
            if data:
                data = json.loads(data)
                result.update({'.'.join(['data', k]): v for k, v in data['attr'].items()})
        except redis.exceptions.RedisError:
            pass
        except KeyError:
            pass

        return result

    def _query_location(self, task_vars):
        db = get_mongodb('data')
        ds = db['device_status']
        result = {}

        try:
            status = ds.find_one({'did': task_vars['did']})
            result['common.location'] = status.get('city', 'guangzhou')
        except KeyError:
            pass

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

    def device_sequence(self, msg):
        content = msg['current']['content']
        try:
            if content['event'] in ['alert', 'fault'] and msg['task_vars'].get(content['attr'], '') != int(content['attr_type']):
                return []
        except ValueError:
            return []

        if content['data'] not in msg['task_vars']:
            next_node = {
                "id": "uuid",
                "params": [content['data']],
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

        flag = True
        if content['step'] > 1:
            try:
                _key = 're_core_{0}_{1}_device_sequence_counter'.format(msg['task_vars']['did'], content['data'])
                cache = get_redis()
                p = cache.pipeline()
                p.incr(_key)
                p.expire(_key, settings.SEQUENCE_EXPIRE)
                res = p.execute()
            except redis.exceptions.RedisError, e:
                msg['error_message'] = 'redis error: {}'.format(str(e))
                msg['task_vars'][content['alias']] = [copy.deepcopy(msg['task_vars'][content['data']])] * content['length']
                flag = False
            else:
                if res[0] > content['step'] * 50:
                    cache.incr(_key, -content['step']*50)

                if 0 != res[0] % content['step']:
                    flag = False

        if flag:
            result = get_sequence('re_core_{0}_{1}_device_sequence'.format(msg['task_vars']['did'], content['data']), content['length'])
            if type(result) is dict:
                msg.updata(result)
                msg['task_vars'][content['alias']] = [copy.deepcopy(msg['task_vars'][content['data']])] * content['length']
            elif not result:
                msg['error_message'] = 'empty stream'
                msg['task_vars'][content['alias']] = [copy.deepcopy(msg['task_vars'][content['data']])] * content['length']
            else:
                msg['task_vars'][content['alias']] = result
        else:
            return []

        return self.next(msg)


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
        if response.status_code > 199 and response.status_code < 300:
            return response.json()['result']
        elif 400 == response.status_code:
            raise Exception(u'script error: {}'.format(response.json()['detail_message']))
        else:
            raise Exception(u'script error')

    def transformation(self, msg):
        content = msg['current']['content']

        if content['data'] not in msg['task_vars']:
            if content['alias'] in msg['custom_vars']:
                next_node = copy.deepcopy(msg['custom_vars'][content['alias']])
                next_node['ports'] = [0]
                next_node['wires'] = [[msg['current']['id']]]
                return [dict(copy.deepcopy(msg), current=next_node)]
            else:
                return []

        msg['task_vars'][content['alias']] = DataTransformer(msg['task_vars'][content['data']]).run(content['func'], content.get('params', {}))

        return self.next(msg)


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
                if 'virtual:site' == msg['task_vars'].get('mac', ''):
                    update_virtual_device_log(msg.get('log_id'), 'triggle', 2, 'time now is not allowed')
                return [], True

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

        extern_list = []
        if 'extern_params' in msg['current']:
            extern_list = [x for x in msg['current']['extern_params'] if x not in msg['extern_params']]

        if extern_list:
            extern_result = self._query_extern(msg['task_vars'], extern_list, content)
            msg['extern_params'].update(extern_result)

        if 'alias' in msg['extern_params']:
            for values in msg['extern_params']['alias'].values():
                emp = filter(lambda x: not x.get('dev_alias', ''), values)
                if emp:
                    if 'common.product_name' in msg['task_vars']:
                        for v in values:
                            if not v.get('dev_alias', ''):
                                v['dev_alias'] = msg['task_vars']['common.product_name']
                    else:
                        query_list.append('common.product_name')
                        break


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

        delay = msg['current'].get('delay', 0)
        if delay > 30:
            _msg = {
                'action_type': 'schedule_wait',
                'product_key': msg['product_key'],
                'did': msg['did'],
                'mac': msg['mac'],
                'rule_id': msg['rule_id'],
                'node_id': msg['current']['id'],
                'msg_to': settings.MSG_TO['external'],
                'ts': msg['sys.timestamp'] + delay,
                'flag': '',
                'once': True,
                'msg': {
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
            }
        else:
            if delay > 0:
                time.sleep(delay)
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

        if 'virtual:site' == msg['task_vars'].get('mac', ''):
            _msg['log_data'] = {
                'log_id': msg.get('log_id'),
                'field': 'action',
                'value': _msg['action_type']
            }

        return [_msg], True

    def _query_extern(self, task_vars, extern_list, content):
        extern_list = list(set(extern_list))
        result = {x: getattr(self, 'extern_' + x)(task_vars, content) for x in extern_list}
        return result

    def extern_alias(self, task_vars, content):
        app_id = content.get('app_id', '')
        uids = content.get('uids', [])
        if not app_id:
            return {}
        url = "http://{0}/v1/bindings/{1}?appids={2}".format(settings.HOST_GET_BINDING, task_vars['did'], app_id)
        headers = {
            'Authorization': settings.INNER_API_TOKEN
        }
        try:
            response = requests.get(url, headers=headers)
            data = json.loads(response.content)
        except:
            data = {}

        return {k: filter(lambda x: x['uid'] in uids, v) for k, v in data.items()} if uids else data
