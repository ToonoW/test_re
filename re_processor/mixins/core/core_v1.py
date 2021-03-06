#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time, copy, requests, redis

from re_processor import settings
from re_processor.connections import get_mongodb, get_mysql, redis_pool, get_redis_la
from re_processor.common import _log, logger


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

def get_value_from_task(name, task_vars):
    if task_vars.has_key(name):
        value = task_vars.get(name, None)
    else:
        value = get_value_from_json(name, task_vars.get(name.split('.')[0], {}))
    return value

class BaseCore(object):
    '''
    base class
    '''

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list)
        '''
        #print 'running {}'.format(self.core_name)
        return self._process(msg)


class BaseInnerCore(object):
    '''
    base class
    '''

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list)
        '''
        #print 'running {}'.format(self.core_name)
        task_list, task_vars, custom_vars, para_task, extern_params = msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['para_task'], msg.get('extern_params', {})

        result = self._process(task_list, task_vars, custom_vars, para_task, extern_params, msg['rule_id'])

        if result is False and para_task:
            task_list = para_task.pop()
            result = True

        msg['task_list'], msg['current'], msg['para_task'], msg['extern_params'] = task_list, (task_list[0][0] if task_list else 'tri'), (para_task if task_list else []), extern_params

        return result, ([msg] if result else [])


class SelectorCore(BaseInnerCore):
    '''
    execute cmp task
    '''
    core_name = 'sel'
    opt = {
        '>': operator.__gt__,
        '<': operator.__lt__,
        '==': operator.__eq__,
        '>=': operator.__ge__,
        '<=': operator.__le__,
        '!=': operator.__ne__
    }
    index = settings.INDEX['sel']
    params = ['left', 'opt', 'right']
    pattern = {
        'number': re.compile(r'^[0-9]+(\.[0-9]+)*$'),
        'string': re.compile(r'^(\'.+\'|".+")$'),
        'hex': re.compile(r'^0(x|X)[0-9a-fA-F]+$')
    }

    def _process(self, task_list, task_vars, custom_vars, para_task, extern_params, rule_id):
        result = True
        while result and task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                break

            extra_task = []
            query_list = []
            tmp_dict = {}
            hex_flag = False
            for symbol in self.params:
                tmp = task[self.index[symbol]]
                if self.opt.has_key(tmp):
                    tmp = self.opt[tmp]
                elif task_vars.has_key(tmp):
                    tmp = task_vars[tmp]
                elif custom_vars.has_key(tmp):
                    extra_task.append(custom_vars[tmp])
                elif self.pattern['number'].search(tmp):
                    tmp = json.loads(tmp)
                elif self.pattern['string'].search(tmp):
                    tmp = tmp[1:-1]
                elif self.pattern['hex'].search(tmp):
                    hex_flag = True
                elif tmp.split('.')[0] in custom_vars:
                    alias = tmp.split('.')[0]
                    if alias in task_vars:
                        tmp = get_value_from_json(tmp, task_vars[alias])
                    else:
                        extra_task.append(custom_vars[alias])
                else:
                    query_list.append(tmp)
                tmp_dict[symbol] = tmp

            if extra_task or query_list:
                task_list[0:0] = ([['que', 'q', list(set(query_list)), False]] if query_list else []) + extra_task + [task]
                break

            if hex_flag:
                tmp_dict['left'] = int(tmp_dict['left'], 16)
                tmp_dict['right'] = int(tmp_dict['right'], 16)

            result = tmp_dict['opt'](tmp_dict['left'], tmp_dict['right'])

        return result


class CalculatorCore(BaseInnerCore):
    '''
    execute cal task
    '''

    core_name = 'cal'
    opt = {
        '+': operator.__add__,
        '-': operator.__sub__,
        '*': operator.__mul__,
        '/': operator.__div__
    }
    index = settings.INDEX['cal']
    params = ['exp', 'name']
    pattern = {
        'number': re.compile(r'^[0-9]+(\.[0-9]+)*$'),
        'string': re.compile(r'^(\'.+\'|".+")$'),
        'hex': re.compile(r'^0(x|X)[0-9a-fA-F]+$')
    }

    def _process(self, task_list, task_vars, custom_vars, para_task, extern_params, rule_id):
        result = False
        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                result = True
                break
            extra_task = []
            query_list = []
            exp = []
            tmp_dict = {x: task[self.index[x]] for x in self.params}

            for symbol in tmp_dict['exp']:
                if self.opt.has_key(symbol):
                    exp.append(symbol)
                elif task_vars.has_key(symbol):
                    exp.append(float(task_vars[symbol]))
                elif custom_vars.has_key(symbol):
                    extra_task.append(custom_vars[symbol])
                elif self.pattern['number'].search(symbol):
                    exp.append(json.loads(symbol))
                elif symbol.split('.')[0] in custom_vars:
                    alias = symbol.split('.')[0]
                    if alias in task_vars:
                        exp.append(get_value_from_json(symbol, task_vars[alias]))
                    else:
                        extra_task.append(custom_vars[alias])
                else:
                    query_list.append(symbol)

            if extra_task or query_list:
                task_list[0:0] = ([['que', 'q', list(set(query_list)), False]] if query_list else []) + extra_task + [task]
                result = True
                break

            try:
                res = reduce(self._calculate, exp, [0])
                result = True
            except ZeroDivisionError:
                break
            task_vars[tmp_dict['name']] = res.pop()

        return result

    def _calculate(self, params_stack, symbol):
        if self.opt.has_key(symbol):
            left, right = params_stack[-2:]
            params_stack = params_stack[0:-2]
            params_stack.append(self.opt[symbol](left, right))
        else:
            params_stack.append(symbol)

        return params_stack


class ScriptCore(BaseInnerCore):
    '''
    execute script task
    '''

    core_name = 'script'
    params = ['script_id', 'params', 'name']
    index = settings.INDEX['script']

    def _process(self, task_list, task_vars, custom_vars, para_task, extern_params, rule_id):
        result = False
        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                result = True
                break

            extra_task = []
            query_list = []
            params = {}
            tmp_dict = {x: task[self.index[x]] for x in self.params}

            for symbol in tmp_dict['params']:
                if task_vars.has_key(symbol):
                    params[symbol] = task_vars[symbol]
                elif custom_vars.has_key(symbol):
                    extra_task.append(custom_vars[symbol])
                elif symbol.split('.')[0] in custom_vars:
                    alias = symbol.split('.')[0]
                    if alias in task_vars:
                        params[symbol] = get_value_from_json(symbol, task_vars[alias])
                    else:
                        extra_task.append(custom_vars[alias])
                else:
                    query_list.append(symbol)

            if extra_task or query_list:
                task_list[0:0] = ([['que', 'q', list(set(query_list)), False]] if query_list else []) + extra_task + [task]
                result = True
                break

            task_vars[tmp_dict['name']] = self.run(tmp_dict['script_id'], params)
            result = True

        return result

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


class JsonCore(BaseInnerCore):
    '''
    execute script task
    '''

    core_name = 'json'
    params = ['source', 'params', 'refresh', 'content', 'name']
    index = settings.INDEX['json']

    def _process(self, task_list, task_vars, custom_vars, para_task, extern_params, rule_id):
        result = False
        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                result = True
                break

            extra_task = []
            query_list = []
            params = {}
            tmp_dict = {x: task[self.index[x]] for x in self.params}
            for symbol in tmp_dict['params']:
                if task_vars.has_key(symbol):
                    params[symbol] = task_vars[symbol]
                elif custom_vars.has_key(symbol):
                    extra_task.append(custom_vars[symbol])
                elif symbol.split('.')[0] in custom_vars:
                    alias = symbol.split('.')[0]
                    if alias in task_vars:
                        params[symbol] = get_value_from_json(symbol, task_vars[alias])
                    else:
                        extra_task.append(custom_vars[alias])
                else:
                    query_list.append(symbol)

            if extra_task or query_list:
                task_list[0:0] = ([['que', 'q', list(set(query_list)), False]] if query_list else []) + extra_task + [task]
                result = True
                break

            task_vars[tmp_dict['name']] = self.get_json(tmp_dict['content'], params, int(tmp_dict['refresh']), tmp_dict['name'], rule_id, task_vars['sys.timestamp_ms'])
            result = True

        return result

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
        except Exception, e:
            logger.exception(e)

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
            except Exception, e:
                logger.exception(e)
        else:
            try:
                result = cache.get(cache_key)
                result = json.loads(result)
            except Exception, e:
                logger.exception(e)

        return result


class QueryCore(BaseInnerCore):
    '''
    execute que task
    '''

    core_name = 'que'
    index = settings.INDEX['que']
    params = ['type', 'target', 'pass']

    def _process(self, task_list, task_vars, custom_vars, para_task, extern_params, rule_id):
        result = False
        params_list = []
        extern_list = []
        pass_flag = False

        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                result = True
                break
            tmp_dict = {x: task[self.index[x]] for x in self.params if self.index[x] < len(task)}
            if 'q' == tmp_dict['type']:
                params_list.extend(tmp_dict['target'])
                pass_flag = tmp_dict.get('pass', False)

            elif 'e' == tmp_dict['type']:
                for target in tmp_dict['target']:
                    if not extern_params.has_key(target):
                        extern_list.append(target)

        if extern_list:
            extern_result = self._query_extern(task_vars, extern_list)
            extern_params.update(extern_result)
            result = True

        if params_list:
            query_result = self._query(task_vars, params_list)
            not_found = filter(lambda x: x not in query_result and x not in task_vars, params_list)
            if query_result and not not_found:
                task_vars.update(query_result)
                result = True
            elif pass_flag:
                task_vars.update(query_result)
                task_vars.update({x: '' for x in not_found})
                result = True
            else:
                result = False

        return result

    def _query_extern(self, task_vars, extern_list):
        extern_list = list(set(extern_list))
        result = {x: getattr(self, 'extern_' + x)(task_vars) for x in extern_list}
        return result

    def extern_alias(self, task_vars):
        url = "{0}{1}{2}{3}".format('http://', settings.HOST_GET_BINDING, '/v1/bindings/', task_vars['did'])

        headers = {
            'Authorization': settings.INNER_API_TOKEN
        }
        try:
            response = requests.get(url, headers=headers)
            data = json.loads(response.content)
        except:
            data = {}

        for values in data.values():
            emp = filter(lambda x: not x.get('dev_alias', ''), values)
            if emp:
                if 'common.product_name' not in task_vars:
                    task_vars.update(self._query_product_name(task_vars))

                for v in values:
                    if not v.get('dev_alias', ''):
                        v['dev_alias'] = task_vars['common.product_name']

        return data

    def _query(self, task_vars, params_list):
        result = {}
        prefix = list(set([x.split('.')[0] for x in params_list]))

        if 'data' in prefix:
            result.update(self._query_data(task_vars))

        if 'common.location' in params_list:
            result.update(self._query_location(task_vars))

        if 'display' in prefix:
            result.update(self._query_display(task_vars))

        if 'common.product_name' in params_list and 'common.product_name' not in task_vars:
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
        except Exception, e:
            logger.exception(e)

        return result

    def _query_location(self, task_vars):
        db = get_mongodb('data')
        ds = db['device_status']
        result = {}

        try:
            status = ds.find_one({'did': task_vars['did']})
            result['common.location'] = status.get('city', 'guangzhou')
        except Exception, e:
            logger.exception(e)

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
        except Exception, e:
            logger.exception(e)

        return result


class TriggerCore(BaseCore):
    '''
    execute tri task
    '''

    core_name = 'tri'
    index = settings.INDEX['tri']
    db_index = settings.INDEX['tri_in_db']
    params = ['action_type', 'params', 'extern_params', 'action_content', 'action_id']
    db_params = ['allow_time', 'task_list', 'action_type', 'params', 'action_content']

    def _process(self, msg):
        task_list, task_vars, custom_vars, extern_params = msg['task_list'], msg['task_vars'], msg['custom_vars'], msg.get('extern_params', {})
        msg_list = []

        if not task_list:
            db = get_mysql()
            sql = 'select `id`, `action_tree`, `extern_params`, `name` from `{0}` where `rule_id_id`={1}'.format(
                settings.MYSQL_TABLE['action']['table'], msg['rule_id'])
            db.execute(sql)
            for action_id, action_tree, extern_params_db, name in db.fetchall():
                action_tree = json.loads(action_tree)
                extern_params_db = json.loads(extern_params_db) if extern_params_db else []
                tmp_dict = {x: action_tree[self.db_index[x]] for x in self.db_params}

                if tmp_dict['allow_time']:
                    time_now = map(int, time.strftime('%m-%d-%H-%w').split('-'))
                    time_month, time_day, time_hour, time_week = tmp_dict['allow_time'].get('month', []), tmp_dict['allow_time'].get('day', []), tmp_dict['allow_time'].get('hour', []), tmp_dict['allow_time'].get('week', [])
                    if (time_month and time_now[0] not in time_month) or \
                            (time_day and time_now[1] not in time_day) or \
                            (time_hour and time_now[2] not in time_hour) or \
                            (time_week and time_now[3] % 7 not in time_week):
                        p_log = {
                            'module': 're_processor',
                            'rule_id': msg.get('rule_id', ''),
                            'action_id': action_id,
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
                        continue

                action_task = ['tri', tmp_dict['action_type'], tmp_dict['params'], extern_params_db, tmp_dict['action_content'], action_id]
                if tmp_dict['task_list']:
                    tmp_dict['task_list'].append(action_task)
                    _msg = {}
                    _msg.update(msg)
                    _msg['task_list'], _msg['task_vars'], _msg['custom_vars'], _msg['current'] = tmp_dict['task_list'], copy.copy(task_vars), custom_vars, tmp_dict['task_list'][0][0]
                    msg_list.append(_msg)
                else:
                    task_list.append(action_task)

            db.close()

        new_task_list = []

        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                break
            tmp_dict = {x: task[self.index[x]] for x in self.params}
            extra_task = []
            query_list = []

            if tmp_dict['extern_params']:
                for x in tmp_dict['extern_params']:
                    if not extern_params.has_key(x):
                        extra_task.append(['que', 'e', list(set(tmp_dict['extern_params'])), True])
                        break

            for symbol in tmp_dict['params']:
                if not task_vars.has_key(symbol):
                    if custom_vars.has_key(symbol):
                        extra_task.append(custom_vars[symbol])
                    elif symbol.split('.')[0] in custom_vars:
                        if symbol.split('.')[0] not in task_vars:
                            extra_task.append(custom_vars[symbol.split('.')[0]])
                    else:
                        query_list.append(symbol)

            if extra_task or query_list:
                new_task_list[0:0] = ([['que', 'q', list(set(query_list)), True]] if query_list else []) + extra_task
                new_task_list.append(task)
                new_task_list.extend(task_list)
                break
            else:
                _msg = {
                    'msg_to': settings.MSG_TO['external'],
                    'action_type': tmp_dict['action_type'],
                    'event': msg.get('event', ''),
                    'rule_id': msg.get('rule_id', ''),
                    'product_key': task_vars.get('product_key', ''),
                    'did': task_vars.get('did', ''),
                    'mac': task_vars.get('mac', ''),
                    'ts': time.time(),
                    'params': {x: get_value_from_task(x, task_vars) for x in tmp_dict['params']},
                    'extern_params': extern_params,
                    'content': tmp_dict['action_content']
                }

                msg_list.append(_msg)

        if new_task_list:
            _msg = {}
            _msg.update(msg)
            _msg['task_list'], _msg['task_vars'],  _msg['current'] = new_task_list, copy.copy(task_vars), new_task_list[0][0]
            msg_list.append(_msg)

        return True if msg_list else False, msg_list
