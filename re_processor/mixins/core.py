#!/usr/bin/env python
# coding=utf-8

import json, operator, re, time

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
        print 'running {}'.format(self.core_name)
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

    def _process(self, msg):
        task_list, task_vars, custom_vars = msg['task_list'], msg['task_vars'], msg['custom_vars']
        result = True
        while result and task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                break

            extra_task = []
            query_list = []
            tmp_dict = {}
            for symbol in self.params:
                tmp = task[self.index[symbol]]
                if self.opt.has_key(tmp):
                    tmp = self.opt[tmp]
                elif task_vars.has_key(tmp):
                    tmp = task_vars[tmp]
                #elif '.' in tmp and task_vars.has_key(tmp.split('.', 1)[1]):
                #    tmp = task_vars[tmp.split('.', 1)[1]]
                elif custom_vars.has_key(tmp):
                    extra_task.append(custom_vars[tmp])
                elif re.search(r'^[0-9]+(\.[0-9]+)*$', tmp):
                    tmp = json.loads(tmp)
                elif re.search(r'^(\'.+\'|".+")$', tmp):
                    tmp = tmp[1:-1]
                else:
                    query_list.append(tmp)
                tmp_dict[symbol] = tmp

            if extra_task or query_list:
                task_list[0:0] = [['que', 'q', query_list]] if query_list else [] + extra_task + [task]
                break

            result = tmp_dict['opt'](tmp_dict['left'], tmp_dict['right'])

        msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['current'] = task_list, task_vars, custom_vars, task_list[0][0] if task_list else 'tri'

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

    def _process(self, msg):
        task_list, task_vars, custom_vars = msg['task_list'], msg['task_vars'], msg['custom_vars']
        result = False
        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
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
                #elif '.' in symbol and task_vars.has_key(symbol.split('.', 1)[1]):
                #    exp.append(float(task_vars[symbol.split('.', 1)[1]]))
                elif custom_vars.has_key(symbol):
                    extra_task.append(custom_vars[symbol])
                elif re.search(r'^[0-9]+(\.[0-9]+)*$', symbol):
                    exp.append(json.loads(symbol))
                else:
                    query_list.append(symbol)

            if extra_task or query_list:
                task_list[0:0] = [['que', 'q', query_list]] if query_list else [] + extra_task + [task]
                break

            try:
                res = reduce(self._calculate, exp, [0])
                result = True
            except ZeroDivisionError:
                break
            task_vars[tmp_dict['name']] = res.pop()

        msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['current'] = task_list, task_vars, custom_vars, task_list[0][0] if task_list else 'tri'

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
    params = ['type', 'target']

    def _process(self, msg):
        task_list, task_vars, custom_vars = msg['task_list'], msg['task_vars'], msg['custom_vars']
        result = False
        params_list = []
        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                result = True
                break
            tmp_dict = {x: task[self.index[x]] for x in self.params}
            if 'q' == tmp_dict['type']:
                params_list.extend(tmp_dict['target'])

        if params_list:
            query_result = self._query(task_vars, params_list)
            if query_result and not filter(lambda x: not query_result.has_key(x), params_list):
                task_vars.update(query_result)
                result = True

        msg['task_list'], msg['task_vars'], msg['custom_vars'], msg['current'] = task_list, task_vars, custom_vars, task_list[0][0] if task_list else 'tri'

        return result, [msg] if result else [], not result

    def _query(self, task_vars, params_list):
        return self._query_data(task_vars, params_list)

    def _query_data(self, task_vars, params_list):
        db = get_mongodb()
        ds = db['device_status']

        try:
            status = ds.find_one({'did': task_vars['did']})
            result = {'.'.join(['data', k]): v for k, v in status['attr']['0'].items()}
            result['online.status'] = 1 if status['is_online'] else 0
            result['offline.status'] = 0 if status['is_online'] else 1
        except KeyError:
            result = {}

        return result


class TriggerCore(BaseCore):
    '''
    execute tri task
    '''

    core_name = 'tri'
    index = settings.INDEX['tri']
    db_index = settings.INDEX['tri_in_db']
    params = ['action_type', 'params', 'action_content', 'action_id']
    db_params = ['allow_time', 'task_list', 'action_type', 'params', 'action_content']

    def _process(self, msg):
        task_list, task_vars, custom_vars = msg['task_list'], msg['task_vars'], msg['custom_vars']
        msg_list = []
        log_flag = False

        if not task_list:
            db = get_mysql()
            sql = 'select `id`, `action_tree` from `{0}` where `rule_id_id`={1}'.format(
                settings.MYSQL_TABLE['action']['table'], msg['rule_id'])
            db.execute(sql)
            for action_id, action_tree in db.fetchall():
                action_tree = json.loads(action_tree)
                tmp_dict = {x: action_tree[self.db_index[x]] for x in self.db_params}
                time_now = map(int, time.strftime('%m-%d-%H-%w').split('-'))
                if time_now[0] not in tmp_dict['allow_time'].get('month', range(1, 13)) or \
                        time_now[1] not in tmp_dict['allow_time'].get('day', range(1, 32)) or \
                        time_now[2] not in tmp_dict['allow_time'].get('hour', range(1, 61)) or \
                        time_now[3] % 7 not in tmp_dict['allow_time'].get('week', range(1, 8)):
                    continue

                action_task = ['tri', tmp_dict['action_type'], tmp_dict['params'], tmp_dict['action_content'], action_id]
                if tmp_dict['task_list']:
                    tmp_dict['task_list'].append(action_task)
                    _msg = {}
                    _msg.update(msg)
                    _msg['task_list'], _msg['task_vars'], _msg['custom_vars'], _msg['current'] = tmp_dict['task_list'], task_vars, custom_vars, tmp_dict['task_list'][0][0]
                    msg_list.append(_msg)
                else:
                    task_list.append(action_task)

            db.close()

        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                break
            tmp_dict = {x: task[self.index[x]] for x in self.params}
            extra_task = []
            query_list = []
            for symbol in tmp_dict['params']:
                if custom_vars.has_key(symbol):
                    extra_task.append(custom_vars[symbol])
                elif not task_vars.has_key(symbol):
                    query_list.append(symbol)
            if extra_task or query_list:
                task_list[0:0] = [['que', 'q', query_list]] if query_list else [] + extra_task + [task]
                _msg = {}
                _msg.update(msg)
                _msg['task_list'], _msg['task_vars'], _msg['custom_vars'], _msg['current'] = task_list, task_vars, custom_vars, task_list[0][0]
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
                    'content': tmp_dict['action_content']
                }
                if msg.get('debug') is True and msg['debug'] is True:
                    _msg['debug'] = True
                    _msg['rule_id'] = msg.get('rule_id', '')
                    _msg['test_id'] = msg.get('test_id', '')
                    _msg['action_id'] = tmp_dict['action_id']
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
