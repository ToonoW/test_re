#!/usr/bin/env python
# coding=utf-8

import json, operator, re

import settings
from connections import get_mongodb


class BaseCore(object):
    '''
    base class
    '''

    def process(self, task_list, task_vars, custom_vars):
        '''
        execute task, return a three-tuple (result, data, task_list)
        '''
        pass


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

    def process(self, task_list, task_vars, custom_vars):
        result = True
        while task_list:
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
                elif task_vars.has_key(tmp.split('.')[1]):
                    tmp = task_vars[tmp.split('.')[1]]
                elif custom_vars.has_key(tmp):
                    extra_task.append(custom_vars[tmp])
                elif re.search(r'^([0-9]+(\.[0-9]+)*|true|false)$', tmp):
                    tmp = json.dumps(tmp)
                else:
                    query_list.append(tmp)
                tmp_dict[symbol] = tmp

            if extra_task or query_list:
                task_list[0:0] = ['que', 'q', query_list] + extra_task + [task]
                break

            result = tmp_dict['opt'](tmp_dict['left'], tmp_dict['right'])
            if result is not True:
                break

        return result, task_vars, task_list


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

    def process(self, task_list, task_vars, custom_vars):
        result = True
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
                if task_vars.has_key(symbol):
                    exp.append(float(task_vars[symbol]))
                elif task_vars.has_key(symbol.split('.')[1]):
                    exp.append(float(task_vars[symbol.split('.')[1]]))
                elif custom_vars.has_key(symbol):
                    extra_task.append(custom_vars[symbol])
                elif re.search(r'^[0-9]+(\.[0-9]+)*$', symbol):
                    symbol = json.dumps(symbol)
                else:
                    query_list.append(symbol)

            if extra_task or query_list:
                task_list[0:0] = ['que', 'q', query_list] + extra_task + [task]
                break

            res = reduce(self._calculate, exp, [0])
            task_vars[tmp_dict['name']] = res.pop()

        return result, task_vars, task_list

    def _calculate(self, params_stack, symbol):
        if self.opt.has_key(symbol):
            left, right = params_stack[0:2]
            params_stack = params_stack[2:]
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

    def process(self, task_list, task_vars, custom_vars):
        result = True
        params_list = []
        while task_list:
            task = task_list.pop(0)
            if self.core_name != task[0]:
                task_list[0:0] = [task]
                break
            tmp_dict = {x: task[self.index[x]] for x in self.params}
            if 'q' == tmp_dict['type']:
                params_list.extend(tmp_dict['target'])

        if params_list:
            query_result = self._query(task_vars, params_list)
            if query_result:
                task_vars.update(query_result)
            else:
                result = False

        return result, task_vars, task_list

    def _query(self, task_vars, params_list):
        result = {}
        if 'unbind' not in params_list and 'bind' not in params_list:
            data = self._query_data(task_vars, params_list)
            if data:
                result.update(data)
        return result

    def _query_data(self, task_vars, params_list):
        db = get_mongodb()
        ds = db['device_status']

        try:
            status = ds.find_one({'did': task_vars['did']})
            result = status['attr']['0']
            result['online.status'] = status['is_online']
            result['offline.status'] = not status['is_online']
        except KeyError:
            result = {}

        return result
