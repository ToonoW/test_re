#!/usr/bin/env python
# coding=utf-8
from re_processor.sender import MainSender
import operator
import json

def operate_calc(msg_list, dp):
    opt = {
        '>': operator.__gt__,
        '<': operator.__lt__,
        '==': operator.__eq__,
        '>=': operator.__ge__,
        '<=': operator.__le__,
        '!=': operator.__ne__
    }
    wires = []
    for msg in msg_list:
        func_list = msg['func_content']
        for func in func_list:
            operation = opt.get(func['opt'])
            if operation:
                cond1 = func['cond1'].replace("data.", "")
                cond2 = func['cond2']
                dp_value = dp.get(cond1)
                if dp_value:
                    compare = operation(dp_value, float(cond2))
                    if not compare:
                        break
                    if func.get('wires'):
                        wires.append(func.get('wires'))
    return wires


def send_output_msg(output_list, wires, msg, log):
    product_key = msg['product_key']
    for output in output_list:
        if output['id'] in wires:
            sender = MainSender(product_key)
            content = output.get('content')
            message = {
                "product_key": product_key,
                "did": msg['did'],
                "mac": msg['mac'],
                "params": {},
                "app_id": content.get('app_id'),
                "action_type": output['type'],
                "extern_params": content.get('custom_params'),
                "content": json.dumps({
                    "title": content.get('title', '通知'),
                    "ptype": content.get('ptype'),
                    "english_template": content.get("english_template"),
                    "template": content.get("template")
                })
            }
            sender.send(message, product_key)
