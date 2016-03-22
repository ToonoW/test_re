#!/usr/bin/env python
# coding=utf-8

import json, operator, re

import settings


class BaseCore(object):
    '''
    base class
    '''

    def process(self, task_list, vars, custom_vars):
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

    def process(self, task_list, vars, custom_vars):
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
                elif vars.has_key(tmp):
                    tmp = vars[tmp]
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
            else:
                result = tmp_dict['opt'](tmp_dict['left'], tmp_dict['right'])
                if result is not True:
                    break

        return result, vars, task_list


class CalculatorCore(BaseCore):
    '''
    execute cal task
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
    index = settings.INDEX['cal']
    params = ['exp', 'name']

    def process(self, task_list, vars, custom_vars):
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
                tmp_dict[symbol] = symbol

            for symbol in tmp_dict['exp']:
                pass
