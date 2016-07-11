#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time, copy, requests

from re_processor import settings
from re_processor.connections import get_mongodb, get_mysql
from re_processor.common import _log


class BaseCore(object):
    '''
    base class
    '''

    def process(self, msg):
        '''
        execute task, return a three-tuple (result, msg_list, log_flag)
        '''
        #print 'running {}'.format(self.core_name)
        return self._process(msg)


class SelectorCore(BaseCore):
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

    def _process(self, msg):
        task_list, task_vars, custom_vars, para_task = msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['para_task']
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

        if result is False and para_task:
            task_list = para_task.pop()
            result = True

        msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['current'], msg['para_task'] = task_list, task_vars, custom_vars, task_list[0][0] if task_list else 'tri', para_task if task_list else []

        return result, [msg] if result else [], 'tri' == msg['current']


class CalculatorCore(BaseCore):
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

    def _process(self, msg):
        task_list, task_vars, custom_vars, para_task = msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['para_task']
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

        if result is False and para_task:
            task_list = para_task.pop()
            result = True

        msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['current'], msg['para_task'] = task_list, task_vars, custom_vars, task_list[0][0] if task_list else 'tri', para_task if task_list else []

        return result, [msg] if result else [], not result

    def _calculate(self, params_stack, symbol):
        if self.opt.has_key(symbol):
            left, right = params_stack[-2:]
            params_stack = params_stack[0:-2]
            params_stack.append(self.opt[symbol](left, right))
        else:
            params_stack.append(symbol)

        return params_stack


class QueryCore(BaseCore):
    '''
    execute que task
    '''

    core_name = 'que'
    index = settings.INDEX['que']
    params = ['type', 'target', 'pass']

    def _process(self, msg):
        task_list, task_vars, custom_vars, para_task, extern_params = msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['para_task'], msg.get('extern_params', {})
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
            not_found = filter(lambda x: not query_result.has_key(x), params_list)
            if query_result and not not_found:
                task_vars.update(query_result)
                result = True
            elif pass_flag:
                task_vars.update(query_result)
                task_vars.update({x: '' for x in not_found})
                result = True
            else:
                result = False

        if result is False and para_task:
            task_list = para_task.pop()
            result = True

        msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['current'], msg['para_task'], msg['extern_params'] = task_list, task_vars, custom_vars, task_list[0][0] if task_list else 'tri', para_task if task_list else [], extern_params

        return result, [msg] if result else [], not result

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

        return data

    def _query(self, task_vars, params_list):
        result = {}
        prefix = list(set([x.split('.')[0] for x in params_list]))

        if 'data' in prefix:
            result.update(self._query_data(task_vars))
        if 'display' in prefix or 'common.product_name' in params_list:
            result.update(self._query_display(task_vars))

        return result

    def _query_data(self, task_vars):
        db = get_mongodb('data')
        ds = db['device_status']

        try:
            status = ds.find_one({'did': task_vars['did']})
            result = {'.'.join(['data', k]): v for k, v in status['attr']['0'].items()}
            result['online.status'] = 1 if status['is_online'] else 0
            result['offline.status'] = 0 if status['is_online'] else 1
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
        log_flag = False

        if not task_list:
            db = get_mysql()
            sql = 'select `id`, `action_tree`, `extern_params` from `{0}` where `rule_id_id`={1}'.format(
                settings.MYSQL_TABLE['action']['table'], msg['rule_id'])
            db.execute(sql)
            for action_id, action_tree, extern_params_db in db.fetchall():
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
                            'msg_to': settings.MSG_TO['internal'],
                            'module': 're_processor',
                            'rule_id': msg.get('rule_id', ''),
                            'action_id': action_id,
                            'event': msg.get('event', ''),
                            'product_key': msg['task_vars'].get('product_key', ''),
                            'did': msg['task_vars'].get('did', ''),
                            'mac': msg['task_vars'].get('mac', ''),
                            'current': 'log',
                            'time_now': 'month: {0}, day: {1}, hour: {2}, week: {3}'.format(*time_now),
                            'result': 'failed',
                            'handling': 'action',
                            'error_message': 'time now is not in list of allow_time'
                        }
                        msg_list.append(p_log)
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
                    else:
                        query_list.append(symbol)

            if extra_task or query_list:
                new_task_list[0:0] = ([['que', 'q', list(set(query_list)), True]] if query_list else []) + extra_task
                new_task_list.append(task)
            else:
                log_flag = True
                _msg = {
                    'msg_to': settings.MSG_TO['external'],
                    'action_type': tmp_dict['action_type'],
                    'event': msg.get('event', ''),
                    'product_key': task_vars.get('product_key', ''),
                    'did': task_vars.get('did', ''),
                    'mac': task_vars.get('mac', ''),
                    'ts': time.time(),
                    'params': {x: task_vars[x] for x in tmp_dict['params']},
                    'extern_params': extern_params,
                    'content': tmp_dict['action_content']
                }
                if msg.get('debug') is True and msg['debug'] is True:
                    _msg['debug'] = True
                    _msg['rule_id'] = msg.get('rule_id', '')
                    _msg['test_id'] = msg.get('test_id', '')
                    _msg['action_id'] = tmp_dict['action_id']

                msg_list.append(_msg)

        if new_task_list:
            _msg = {}
            _msg.update(msg)
            _msg['task_list'], _msg['task_vars'],  _msg['current'] = new_task_list, copy.copy(task_vars), new_task_list[0][0]
            msg_list.append(_msg)


        return True if msg_list else False, msg_list, log_flag


class LoggerCore(BaseCore):
    '''
    execute tri task
    '''

    core_name = 'log'

    def _process(self, msg):
        msg.pop('msg_to')
        msg.pop('current')
        _log(msg)

        if msg.get('debug', False) is True:
            msg['msg_to'] = settings.MSG_TO['external']
            return True, [msg], False

        return True, [], False
